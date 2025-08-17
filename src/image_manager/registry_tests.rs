use std::path::Path;
use std::str::FromStr;

use croner::Cron;

use crate::helpers::DataSize;
use crate::image::Image;
use crate::image_manager::{BuildRequest, ImageManager, ImageManagerConfig, Reference};
use crate::reference::ImageTag;
use crate::registry::config::RegistryUpstreamConfig;

#[tokio::test]
async fn test_pull() {
    use std::net::SocketAddr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();
    let tmp_registry_folder = helpers::get_temp_folder();

    let address: SocketAddr = "0.0.0.0:9560".parse().unwrap();

    let image_tag = ImageTag::with_registry(&address.to_string(), "test", "latest");

    // Build image inside registry
    let image = {
        let config = ImageManagerConfig::with_base_folder(tmp_registry_folder.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        let image = build_test_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple4.labarfile"),
            image_tag.clone()
        ).unwrap();
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&image.hash.clone().to_ref()).ok());

        image
    };

    tokio::spawn(crate::registry::run(create_registry_config(address, &tmp_registry_folder)));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        // Login
        let login_result = image_manager.login(&address.to_string(), "guest", "guest").await;
        assert!(login_result.is_ok(), "{}", login_result.unwrap_err());

        // Pull
        let pull_result = image_manager.pull(&image_tag, None).await;
        assert!(pull_result.is_ok(), "{}", pull_result.unwrap_err());
        let pull_image = pull_result.unwrap();
        assert_eq!(image, pull_image);

        // List images
        let images = image_manager.list_images();
        assert!(images.is_ok());
        let images = images.unwrap();
        assert_eq!(1, images.len());
        assert_eq!(&image_tag, &images[0].image.tag);

        // Check content
        let reference = image_tag.clone().to_ref();
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&reference).ok());
        let files = image_manager.list_content(&reference).unwrap();
        assert_eq!(vec!["file1.txt".to_owned(), "file2.txt".to_owned()], files);
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_folder);
        std::fs::remove_dir_all(&tmp_registry_folder);
    }
}

#[tokio::test]
async fn test_push_pull() {
    use std::net::SocketAddr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();
    let tmp_registry_folder = helpers::get_temp_folder();

    let address: SocketAddr = "0.0.0.0:9561".parse().unwrap();
    tokio::spawn(crate::registry::run(create_registry_config(address, &tmp_registry_folder)));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        // Login
        let login_result = image_manager.login(&address.to_string(), "guest", "guest").await;
        assert!(login_result.is_ok(), "{}", login_result.unwrap_err());

        let image_tag = ImageTag::with_registry(&address.to_string(), "test", "latest");

        // Build
        let image = build_test_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple4.labarfile"),
            image_tag.clone()
        ).unwrap();
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&image.hash.clone().to_ref()).ok());

        // Push
        let push_result = image_manager.push(&image.tag, None).await;
        assert!(push_result.is_ok(), "{}", push_result.unwrap_err());
        let push_result = push_result.unwrap();
        assert_eq!(1, push_result);

        // List remote
        let remote_images = image_manager.list_images_in_registry(&address.to_string()).await;
        assert!(remote_images.is_ok());
        let remote_images = remote_images.unwrap();
        assert_eq!(1, remote_images.len());
        assert_eq!(&image_tag, &remote_images[0].image.tag);

        // Remove in order to pull
        assert!(image_manager.remove_image(&image.tag).is_ok());

        // Pull
        let pull_result = image_manager.pull(&image.tag, None).await;
        assert!(pull_result.is_ok(), "{}", pull_result.unwrap_err());
        let pull_image = pull_result.unwrap();
        assert_eq!(image, pull_image);

        // List images
        let images = image_manager.list_images();
        assert!(images.is_ok());
        let images = images.unwrap();
        assert_eq!(1, images.len());
        assert_eq!(&image_tag, &images[0].image.tag);

        // Check content
        let reference = Reference::ImageTag(image.tag.clone());
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&reference).ok());
        let files = image_manager.list_content(&reference).unwrap();
        assert_eq!(vec!["file1.txt".to_owned(), "file2.txt".to_owned()], files);
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_folder);
        std::fs::remove_dir_all(&tmp_registry_folder);
    }
}

#[tokio::test]
async fn test_push_pull_with_ref() {
    use std::str::FromStr;
    use std::net::SocketAddr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();
    let tmp_registry_folder = helpers::get_temp_folder();

    let address: SocketAddr = "0.0.0.0:9562".parse().unwrap();
    tokio::spawn(crate::registry::run(create_registry_config(address, &tmp_registry_folder)));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        // Login
        let login_result = image_manager.login(&address.to_string(), "guest", "guest").await;
        assert!(login_result.is_ok(), "{}", login_result.unwrap_err());

        let image_tag = ImageTag::with_registry(&address.to_string(), "remote_image", "latest");

        // Build
        let image_referred = build_test_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple1.labarfile"),
            ImageTag::from_str("test").unwrap()
        ).unwrap();

        let image = build_test_image(
            &mut image_manager,
            Path::new("testdata/definitions/with_image_ref.labarfile"),
            image_tag.clone()
        ).unwrap();
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&image.hash.clone().to_ref()).ok());

        // Push
        let push_result = image_manager.push(&image.tag, None).await;
        assert!(push_result.is_ok(), "{}", push_result.unwrap_err());
        let push_result = push_result.unwrap();
        assert_eq!(3, push_result);

        // List remote
        let remote_images = image_manager.list_images_in_registry(&address.to_string()).await;
        assert!(remote_images.is_ok());
        let remote_images = remote_images.unwrap();
        assert_eq!(1, remote_images.len());
        assert_eq!(&image_tag, &remote_images[0].image.tag);

        // Remove in order to pull
        assert!(image_manager.remove_image(&image.tag).is_ok());
        assert!(image_manager.remove_image(&image_referred.tag).is_ok());

        // Pull
        let pull_result = image_manager.pull(&image.tag, None).await;
        assert!(pull_result.is_ok(), "{}", pull_result.unwrap_err());
        let pull_image = pull_result.unwrap();
        assert_eq!(image, pull_image);

        // List images
        let images = image_manager.list_images();
        assert!(images.is_ok());
        let images = images.unwrap();
        assert_eq!(1, images.len());
        assert_eq!(&image_tag, &images[0].image.tag);

        // Check content
        let reference = Reference::ImageTag(image.tag.clone());
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&reference).ok());
        let files = image_manager.list_content(&reference).unwrap();
        assert_eq!(vec!["file1.txt".to_owned(), "file2.txt".to_owned()], files);
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_folder);
        std::fs::remove_dir_all(&tmp_registry_folder);
    }
}

#[tokio::test]
async fn test_sync() {
    use std::net::SocketAddr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();
    let tmp_primary_registry_folder = helpers::get_temp_folder();
    let tmp_secondary_registry_folder = helpers::get_temp_folder();

    let primary_address: SocketAddr = "0.0.0.0:9563".parse().unwrap();
    let secondary_address: SocketAddr = "0.0.0.0:9564".parse().unwrap();

    // Build image inside primary registry
    let mut image = {
        let config = ImageManagerConfig::with_base_folder(tmp_primary_registry_folder.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        let image = build_test_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple4.labarfile"),
            ImageTag::with_registry(&primary_address.to_string(), "test", "latest")
        ).unwrap();
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&image.hash.clone().to_ref()).ok());

        image
    };

    tokio::spawn(crate::registry::run(create_registry_config(primary_address, &tmp_primary_registry_folder)));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut secondary_registry_config = create_registry_config(secondary_address, &tmp_secondary_registry_folder);
    secondary_registry_config.upstream = Some(
        RegistryUpstreamConfig {
            hostname: primary_address.to_string(),
            username: "guest".to_string(),
            password: "guest".to_string(),

            sync: true,
            sync_at_startup: true,
            sync_interval: Cron::from_str("* * * * *").unwrap(),

            pull_through: false
        }
    );
    tokio::spawn(crate::registry::run(secondary_registry_config));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let image_tag = ImageTag::with_registry(&secondary_address.to_string(), "test", "latest");
    image.tag = image.tag.set_registry(&secondary_address.to_string());

    {
        let config = ImageManagerConfig::with_base_folder(tmp_folder.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        // Login
        let login_result = image_manager.login(&secondary_address.to_string(), "guest", "guest").await;
        assert!(login_result.is_ok(), "{}", login_result.unwrap_err());

        // Wait until image exists
        let t0 = std::time::Instant::now();
        loop {
            if !image_manager.list_images_in_registry(&secondary_address.to_string()).await.unwrap().is_empty() {
                break;
            }

            if t0.elapsed().as_secs_f64() > 2.0 {
                break;
            }

            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }

        // Pull
        let pull_result = image_manager.pull(&image_tag, None).await;
        assert!(pull_result.is_ok(), "{}", pull_result.unwrap_err());
        let pull_image = pull_result.unwrap();
        assert_eq!(image, pull_image);

        // List images
        let images = image_manager.list_images();
        assert!(images.is_ok());
        let images = images.unwrap();
        assert_eq!(1, images.len());
        assert_eq!(&image_tag, &images[0].image.tag);

        // Check content
        let reference = image_tag.clone().to_ref();
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&reference).ok());
        let files = image_manager.list_content(&reference).unwrap();
        assert_eq!(vec!["file1.txt".to_owned(), "file2.txt".to_owned()], files);
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_folder);
        std::fs::remove_dir_all(&tmp_primary_registry_folder);
        std::fs::remove_dir_all(&tmp_secondary_registry_folder);
    }
}

#[tokio::test]
async fn test_pull_through() {
    use std::net::SocketAddr;

    use crate::helpers;
    use crate::image_manager::ConsolePrinter;

    let tmp_folder = helpers::get_temp_folder();
    let tmp_primary_registry_folder = helpers::get_temp_folder();
    let tmp_secondary_registry_folder = helpers::get_temp_folder();

    let primary_address: SocketAddr = "0.0.0.0:9565".parse().unwrap();
    let secondary_address: SocketAddr = "0.0.0.0:9566".parse().unwrap();

    // Build image inside primary registry
    let mut image = {
        let config = ImageManagerConfig::with_base_folder(tmp_primary_registry_folder.clone());
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        let image = build_test_image(
            &mut image_manager,
            Path::new("testdata/definitions/simple4.labarfile"),
            ImageTag::with_registry(&primary_address.to_string(), "test", "latest")
        ).unwrap();
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&image.hash.clone().to_ref()).ok());

        image
    };

    tokio::spawn(crate::registry::run(create_registry_config(primary_address, &tmp_primary_registry_folder)));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let mut secondary_registry_config = create_registry_config(secondary_address, &tmp_secondary_registry_folder);
    secondary_registry_config.upstream = Some(
        RegistryUpstreamConfig {
            hostname: primary_address.to_string(),
            username: "guest".to_string(),
            password: "guest".to_string(),

            sync: false,
            sync_at_startup: true,
            sync_interval: Cron::from_str("* * * * *").unwrap(),

            pull_through: true
        }
    );
    tokio::spawn(crate::registry::run(secondary_registry_config));

    // Wait until registry starts
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;

    let image_tag = ImageTag::with_registry(&secondary_address.to_string(), "test", "latest");
    image.tag = image.tag.set_registry(&secondary_address.to_string());

    {
        let mut config = ImageManagerConfig::with_base_folder(tmp_folder.clone());
        config.upstream_pull_check = 0.05;
        let mut image_manager = ImageManager::with_config(config, ConsolePrinter::new()).unwrap();

        // Login
        let login_result = image_manager.login(&secondary_address.to_string(), "guest", "guest").await;
        assert!(login_result.is_ok(), "{}", login_result.unwrap_err());

        // Pull
        let pull_result = image_manager.pull(&image_tag, None).await;
        assert!(pull_result.is_ok(), "{}", pull_result.unwrap_err());
        let pull_image = pull_result.unwrap();
        assert_eq!(image, pull_image);

        // List images
        let images = image_manager.list_images();
        assert!(images.is_ok());
        let images = images.unwrap();
        assert_eq!(1, images.len());
        assert_eq!(&image_tag, &images[0].image.tag);

        // Check content
        let reference = image_tag.clone().to_ref();
        assert_eq!(Some(DataSize(3003)), image_manager.image_size(&reference).ok());
        let files = image_manager.list_content(&reference).unwrap();
        assert_eq!(vec!["file1.txt".to_owned(), "file2.txt".to_owned()], files);
    }

    #[allow(unused_must_use)] {
        std::fs::remove_dir_all(&tmp_folder);
        std::fs::remove_dir_all(&tmp_primary_registry_folder);
        std::fs::remove_dir_all(&tmp_secondary_registry_folder);
    }
}

#[cfg(test)]
fn build_test_image(image_manager: &mut ImageManager,
                    path: &Path, image_tag: ImageTag) -> Result<Image, String> {
    use crate::image_definition::ImageDefinition;

    let image_definition = ImageDefinition::parse_file_without_context(
        Path::new(path)
    ).map_err(|err| err.to_string())?;

    image_manager.build_image(BuildRequest {
        build_context: Path::new("").to_path_buf(),
        image_definition,
        tag: image_tag,
        force: false,
    }).map_err(|err| err.to_string())
}

#[cfg(test)]
fn create_registry_config(address: std::net::SocketAddr, tmp_registry_folder: &Path) -> crate::registry::RegistryConfig {
    use crate::registry::RegistryConfig;
    use crate::registry::auth::AccessRight;

    RegistryConfig {
        data_path: tmp_registry_folder.to_path_buf(),
        address,
        pending_upload_expiration: 30.0,
        ssl_cert_path: None,
        ssl_key_path: None,
        upstream: None,
        users: vec![
            (
                "guest".to_owned(),
                crate::registry::auth::Password::from_plain_text("guest"),
                vec![AccessRight::List, AccessRight::Download, AccessRight::Upload, AccessRight::Delete]
            )
        ]
    }
}