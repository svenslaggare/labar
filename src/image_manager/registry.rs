use std::path::Path;
use std::collections::HashSet;
use std::iter::FromIterator;

use serde::{Deserialize, Serialize};

use futures::{TryStreamExt};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use tokio_util::codec::{FramedRead, BytesCodec};

use rusoto_core::RusotoError;
use rusoto_s3::{S3Client, PutObjectRequest, S3, StreamingBody, GetObjectRequest};
use rusoto_s3::GetObjectError;

use crate::image::{Layer, LayerOperation, Image};

pub type RegistryError = String;
pub type RepositoryResult<T> = Result<T, RegistryError>;

fn split_bucket_and_key(uri: &str) -> Option<(String, String)> {
    if !uri.starts_with("s3://") {
        return None;
    }

    let uri = uri.replace("s3://", "");
    let parts = uri.split("/").collect::<Vec<_>>();
    if parts.len() < 2 {
        return None;
    }

    Some((parts[0].to_owned(), parts[1..].join("/")))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RegistryState {
    pub layers: Vec<String>,
    pub images: Vec<Image>
}

impl RegistryState {
    pub fn new() -> RegistryState {
        RegistryState {
            layers: Vec::new(),
            images: Vec::new()
        }
    }

    pub fn add_layers(&mut self, layers: &Vec<String>) {
        let existing_layers = HashSet::<String>::from_iter(self.layers.iter().cloned());

        for layer in layers {
            if !existing_layers.contains(layer) {
                self.layers.push(layer.clone());
            }
        }
    }

    pub fn add_image(&mut self, new_image: Image) {
        for image in &self.images {
            if image.tag == new_image.tag {
                return;
            }
        }

        self.images.push(new_image);
    }

    pub fn get_hash(&self, reference: &str) -> Option<String> {
        for image in &self.images {
            if image.tag == reference {
                return Some(image.hash.clone());
            }
        }

        for layer in &self.layers {
            if layer == reference {
                return Some(layer.clone());
            }
        }

        None
    }
}

pub struct RegistryManager {
    pub registry_uri: String,
    s3_client: S3Client
}

impl RegistryManager {
    pub fn new(registry_uri: String, s3_client: S3Client) -> RegistryManager {
        RegistryManager {
            registry_uri,
            s3_client
        }
    }

    async fn file_exist(&self, file: &str) -> RepositoryResult<bool> {
        let (bucket, key) = split_bucket_and_key(file).ok_or_else(|| "Invalid S3 URI.")?;

        let mut get_object_request = GetObjectRequest::default();
        get_object_request.bucket = bucket;
        get_object_request.key = key;

        let result = self.s3_client.get_object(get_object_request).await;

        match result {
            Ok(_) => Ok(true),
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => Ok(false),
            Err(err) => Err(format!("Failed to find status of object due to: {}", err))
        }
    }

    async fn file_size(&self, file: &str) -> RepositoryResult<Option<usize>> {
        let (bucket, key) = split_bucket_and_key(file).ok_or_else(|| "Invalid S3 URI.")?;

        let mut get_object_request = GetObjectRequest::default();
        get_object_request.bucket = bucket;
        get_object_request.key = key;

        let result = self.s3_client.get_object(get_object_request).await;

        match result {
            Ok(output) => Ok(output.content_length.map(|x| x as usize)),
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => Ok(None),
            Err(err) => Err(format!("Failed to find status of object due to: {}", err))
        }
    }

    async fn upload_file(&self, file: &Path, destination: &str) -> RepositoryResult<()> {
        let (bucket, key) = split_bucket_and_key(destination).ok_or_else(|| "Invalid S3 URI.")?;

        let mut put_object_request = PutObjectRequest::default();
        put_object_request.bucket = bucket;
        put_object_request.key = key;

        let upload_file = tokio::fs::File::open(file)
            .await
            .map_err(|err| format!("Failed to open file {} due to: {}", file.to_str().unwrap(), err))?;
        put_object_request.content_length = Some(upload_file.metadata().await.unwrap().len() as i64);

        let upload_file_stream = FramedRead::new(upload_file, BytesCodec::new()).map_ok(|b| b.freeze());
        put_object_request.body = Some(StreamingBody::new(upload_file_stream));

        self.s3_client.put_object(put_object_request)
            .await
            .map_err(|err| format!("Failed to upload {} due to: {}", destination, err))?;

        Ok(())
    }

    async fn upload_text(&self, content: String, destination: &str) -> RepositoryResult<()> {
        let (bucket, key) = split_bucket_and_key(destination).ok_or_else(|| "Invalid S3 URI.")?;

        let mut put_object_request = PutObjectRequest::default();
        put_object_request.bucket = bucket;
        put_object_request.key = key;
        put_object_request.body = Some(StreamingBody::from(content.into_bytes()));

        self.s3_client.put_object(put_object_request)
            .await
            .map_err(|err| format!("Failed to upload file {} due to: {}", destination, err))?;

        Ok(())
    }

    async fn download_text(&self, source: &str) -> RepositoryResult<String> {
        let (bucket, key) = split_bucket_and_key(source).ok_or_else(|| "Invalid S3 URI.")?;

        let mut get_object_request = GetObjectRequest::default();
        get_object_request.bucket = bucket;
        get_object_request.key = key;

        let result = self.s3_client.get_object(get_object_request)
            .await
            .map_err(|err| format!("Could not find file {} due to: {}", source, err))?;

        let body = result.body.ok_or_else(|| format!("No body in file: {}", source))?;
        let mut content = String::new();
        body.into_async_read().read_to_string(&mut content)
            .await
            .map_err(|err| format!("Failed to read content of file {} due to: {}", source, err))?;

        Ok(content)
    }

    async fn download_file(&self, source: &str, destination: &Path) -> RepositoryResult<()> {
        let (bucket, key) = split_bucket_and_key(source).ok_or_else(|| "Invalid S3 URI.")?;

        let mut output_file = tokio::fs::File::create(destination)
            .await
            .map_err(|err| format!("Failed to create file {}, due to: {}", destination.to_str().unwrap(), err))?;

        let mut get_object_request = GetObjectRequest::default();
        get_object_request.bucket = bucket;
        get_object_request.key = key;

        let result = self.s3_client.get_object(get_object_request)
            .await
            .map_err(|err| format!("Could not find file {} due to: {}", source, err))?;

        let body = result.body.ok_or_else(|| format!("No body in file: {}", source))?;
        let num_to_read = result.content_length.unwrap() as usize;

        let mut buffer = vec![0; 4096];
        let mut body_reader = body.into_async_read();

        let mut num_read = 0;
        while num_read < num_to_read {
            let this_read = body_reader.read(&mut buffer)
                .await
                .map_err(|err| format!("Failed to read from remote due to: {}", err))?;

            output_file.write(&buffer[..this_read])
                .await
                .map_err(|err| format!("Failed to write to file due to: {}", err))?;

            num_read += this_read;
        }

        output_file.flush()
            .await
            .map_err(|err| format!("Failed to flush file due to: {}", err))?;

        let output_file_metadata = output_file.metadata()
            .await
            .map_err(|err| format!("Failed to get file metadata: {}", err))?;

        if num_to_read != output_file_metadata.len() as usize {
            return Err(format!("Expected to file to be {} bytes but is {}", num_read, output_file_metadata.len()));
        }

        Ok(())
    }

    pub async fn force_upload_layer(&self, layer: &Layer) -> RepositoryResult<()> {
        let mut exported_layer = layer.clone();
        let s3_base_path = format!("{}/images/{}", self.registry_uri, exported_layer.hash);

        for operation in &mut exported_layer.operations {
            match operation {
                LayerOperation::File { source_path, .. } => {
                    let remote_source_path = format!(
                        "{}/{}",
                        s3_base_path,
                        Path::new(source_path).file_name().unwrap().to_str().unwrap()
                    );

                    println!("\t\t* Uploading file {} -> {}", source_path, remote_source_path);
                    self.upload_file(Path::new(&source_path), &remote_source_path).await?;
                    *source_path = remote_source_path;
                }
                _ => {}
            }
        }

        let manifest_content = serde_json::to_string_pretty(&exported_layer).unwrap();
        self.upload_text(manifest_content, &format!("{}/manifest.json", s3_base_path)).await?;

        Ok(())
    }

    pub async fn upload_layer(&self, layer: &Layer) -> RepositoryResult<bool> {
        if !self.file_exist(&format!("{}/images/{}/manifest.json", self.registry_uri, layer.hash)).await? {
            self.force_upload_layer(layer).await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn download_layer_manifest(&self, hash: &str) -> RepositoryResult<Layer> {
        let s3_base_path = format!("{}/images/{}", self.registry_uri, hash);
        let manifest_text = self.download_text(&format!("{}/manifest.json", s3_base_path))
            .await
            .map_err(|err| format!("Could not find image {} due to: {}", hash, err))?;

        let layer: Layer = serde_json::from_str(&manifest_text)
            .map_err(|err| format!("Could not deserialize image {} due to: {}", hash, err))?;

        Ok(layer)
    }

    pub async fn get_layer_size(&self, hash: &str) -> RepositoryResult<usize> {
        let mut total_size = 0;

        let mut stack = Vec::new();
        stack.push(hash.to_owned());

        while let Some(current_hash) = stack.pop() {
            let layer = self.download_layer_manifest(&current_hash).await?;
            if let Some(parent_hash) = layer.parent_hash {
                stack.push(parent_hash);
            }

            for operation in layer.operations {
                match operation {
                    LayerOperation::Image { hash } => {
                        stack.push(hash);
                    }
                    LayerOperation::File { source_path, .. } => {
                        if let Some(file_size) = self.file_size(&source_path).await? {
                            total_size += file_size;
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(total_size)
    }

    pub async fn download_layer(&self, download_base_dir: &Path, hash: &str) -> RepositoryResult<Layer> {
        let mut layer = self.download_layer_manifest(hash).await?;
        let layer_base_dir = download_base_dir.join(&layer.hash);

        #[allow(unused_must_use)] {
            tokio::fs::create_dir_all(&layer_base_dir).await;
        }

        for operation in &mut layer.operations {
            match operation {
                LayerOperation::File { source_path, .. } => {
                    let local_source_path = layer_base_dir.join(Path::new(source_path).file_name().unwrap());
                    println!("\t\t* Downloading file {} -> {}", source_path, local_source_path.to_str().unwrap());
                    self.download_file(source_path, &local_source_path).await?;
                    *source_path = local_source_path.to_str().unwrap().to_owned();
                },
                _ => {}
            }
        }

        let new_manifest_text = serde_json::to_string_pretty(&layer).unwrap();
        tokio::fs::write(layer_base_dir.join("manifest.json"), new_manifest_text)
            .await
            .map_err(|err| format!("Failed to write manifest due to: {}", err))?;

        Ok(layer)
    }

    pub async fn download_state(&self) -> RepositoryResult<RegistryState> {
        let state_file = format!("{}/state.json", self.registry_uri);
        if self.file_exist(&state_file).await? {
            let state_content = self.download_text(&state_file).await?;
            Ok(serde_json::from_str(&state_content).unwrap_or_else(|_| RegistryState::new()))
        } else {
            Ok(RegistryState::new())
        }
    }

    pub async fn upload_state(&self, new_state: &RegistryState) -> RepositoryResult<()> {
        self.upload_text(
            serde_json::to_string_pretty(new_state).unwrap(),
            &format!("{}/state.json", self.registry_uri)
        ).await?;

        Ok(())
    }
}

