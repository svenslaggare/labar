use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Local};
use rusqlite::{Connection, OptionalExtension};

use crate::image::{Image, Layer};
use crate::image_manager::ImageManagerResult;
use crate::image_manager::unpack::Unpacking;
use crate::reference::{ImageId, ImageTag};

pub type SqlResult<T> = rusqlite::Result<T>;

pub struct StateManager {
    base_folder: PathBuf,
    pool: Arc<StateSessionPool>
}

impl StateManager {
    pub fn new(base_folder: &Path) -> SqlResult<StateManager> {
        if !base_folder.exists() {
            std::fs::create_dir_all(base_folder).map_err(|_| rusqlite::Error::InvalidPath(base_folder.to_path_buf()))?;
        }

        let connection = StateManager::open_connection(base_folder)?;
        connection.execute(
            r#"
            CREATE TABLE IF NOT EXISTS layers(
                hash TEXT PRIMARY KEY,
                metadata JSONB
            );
            "#,
            ()
        )?;

        connection.execute(
            r#"
            CREATE TABLE IF NOT EXISTS images(
                tag TEXT PRIMARY KEY,
                hash TEXT,
                FOREIGN KEY(hash) REFERENCES layers(hash) ON DELETE CASCADE
            );
            "#,
            ()
        )?;

        connection.execute(
            r#"
            CREATE TABLE IF NOT EXISTS unpackings(
                destination TEXT PRIMARY KEY,
                hash TEXT,
                time TIMESTAMPTZ,
                FOREIGN KEY(hash) REFERENCES layers(hash) ON DELETE RESTRICT
            );
            "#,
            ()
        )?;

        connection.execute(
            r#"
            CREATE TABLE IF NOT EXISTS logins(
                registry TEXT PRIMARY KEY,
                username TEXT,
                password TEXT
            );
            "#,
            ()
        )?;

        connection.execute(
            r#"
            CREATE TABLE IF NOT EXISTS content_hash_cache(
                file TEXT,
                modified INTEGER,
                hash TEXT,
                PRIMARY KEY (file, modified)
            );
            "#,
            ()
        )?;

        connection.execute(
            r#"
            CREATE TABLE IF NOT EXISTS registry_pending_layer_uploads(
                hash TEXT PRIMARY KEY,
                layer_metadata JSONB,
                last_updated TIMESTAMPTZ,
                upload_id TEXT,
                state TEXT
            );
            "#,
            ()
        )?;

        connection.execute(
            r#"
            CREATE INDEX IF NOT EXISTS index_registry_pending_layer_uploads_upload_id ON registry_pending_layer_uploads(upload_id);
            "#,
            ()
        )?;

        connection.execute(
            r#"
            CREATE TABLE IF NOT EXISTS registry_users(
                username TEXT PRIMARY KEY,
                password TEXT NOT NULL,
                access_rights JSONB NOT NULL
            );
            "#,
            ()
        )?;

        Ok(
            StateManager {
                base_folder: base_folder.to_path_buf(),
                pool: Arc::new(StateSessionPool::new(vec![StateSession { connection }]))
            }
        )
    }

    pub fn session(&self) -> SqlResult<StateSession> {
        Ok(
            StateSession {
                connection: StateManager::open_connection(&self.base_folder)?
            }
        )
    }

    fn open_connection(base_folder: &Path) -> SqlResult<Connection> {
        Connection::open(base_folder.join("state.sqlite3"))
    }

    pub fn pooled_session(&self) -> SqlResult<PooledStateSession> {
        if let Some(session) = self.pool.get_session() {
            Ok(PooledStateSession::new(self.pool.clone(), session))
        } else {
            Ok(PooledStateSession::new(self.pool.clone(), self.session()?))
        }
    }
}

pub struct StateSession {
    pub connection: Connection
}

impl StateSession {
    pub fn add_login(&mut self, registry: &str, username: &str, password: &str) -> SqlResult<()> {
        let transaction = self.connection.transaction()?;
        transaction.execute("DELETE FROM logins WHERE registry=?1", (registry, ))?;
        transaction.execute("INSERT INTO logins (registry, username, password) VALUES (?1, ?2, ?3)", (registry, username, password))?;
        transaction.commit()?;
        Ok(())
    }

    pub fn get_login(&self,  registry: &str) -> SqlResult<Option<(String, String)>> {
        self.connection.query_row(
            "SELECT username, password FROM logins WHERE registry=?1",
            [registry],
            |row| Ok((row.get(0)?, row.get(1)?))
        ).optional()
    }

    pub fn all_layers(&self) -> SqlResult<Vec<Layer>> {
        let mut statement = self.connection.prepare("SELECT metadata FROM layers")?;

        let mut layers = Vec::new();
        for layer in statement.query_map([], |row| row.get::<_, Layer>(0))? {
            layers.push(layer?);
        }

        Ok(layers)
    }

    pub fn get_layer(&self,  hash: &ImageId) -> SqlResult<Option<Layer>> {
        self.connection.query_row(
            "SELECT metadata FROM layers WHERE hash=?1",
            [hash.to_string()],
            |row| row.get(0)
        ).optional()
    }

    pub fn layer_exists(&self, hash: &ImageId) -> SqlResult<bool> {
        StateSession::layer_exists_internal(&self.connection, hash)
    }

    fn layer_exists_internal(connection: &Connection, hash: &ImageId) -> SqlResult<bool> {
        let count = connection.query_one(
            "SELECT COUNT(*) FROM layers WHERE hash=?1",
            [hash],
            |row| row.get::<_, i64>(0)
        )?;

        Ok(count > 0)
    }

    pub fn insert_layer(&self, layer: &Layer) -> SqlResult<()> {
        StateSession::insert_layer_internal(&self.connection, layer)
    }

    pub fn insert_or_replace_layer(&mut self, layer: &Layer) -> SqlResult<()> {
        let transaction = self.connection.transaction()?;

        if StateSession::layer_exists_internal(&transaction, &layer.hash)? {
            transaction.execute(
                "UPDATE layers set metadata=?2 WHERE hash=?1",
                (&layer.hash, &serde_json::to_value(&layer).unwrap())
            )?;
        } else {
            StateSession::insert_layer_internal(&transaction, layer)?;
        }

        transaction.commit()?;
        Ok(())
    }

    fn insert_layer_internal(connection: &Connection, layer: &Layer) -> SqlResult<()> {
        connection.execute(
            "INSERT INTO layers (hash, metadata) VALUES (?1, ?2)",
            (&layer.hash, &serde_json::to_value(&layer).unwrap())
        )?;
        Ok(())
    }

    pub fn remove_layer(&self, hash: &ImageId) -> SqlResult<bool> {
        let removed = self.connection.execute("DELETE FROM layers WHERE hash=?1", (&hash, ))? > 0;
        Ok(removed)
    }

    pub fn all_images(&self) -> SqlResult<Vec<Image>> {
        let mut statement = self.connection.prepare("SELECT hash, tag FROM images")?;

        let mut images = Vec::new();
        for image in statement.query_map([], |row| Image::from_row(&row))? {
            images.push(image?);
        }

        Ok(images)
    }

    pub fn get_image(&self, tag: &ImageTag) -> SqlResult<Option<Image>> {
        StateSession::get_image_internal(&self.connection, tag)
    }

    fn get_image_internal(connection: &Connection, tag: &ImageTag) -> SqlResult<Option<Image>> {
        connection.query_row(
            "SELECT hash, tag FROM images WHERE tag=?1",
            [tag],
            |row| Image::from_row(&row)
        ).optional()
    }

    #[allow(dead_code)]
    pub fn image_exists(&self, tag: &ImageTag) -> SqlResult<bool> {
        StateSession::image_exists_internal(&self.connection, tag)
    }

    fn image_exists_internal(connection: &Connection, tag: &ImageTag) -> SqlResult<bool> {
        let count = connection.query_one(
            "SELECT COUNT(*) FROM images WHERE tag=?1",
            [tag],
            |row| row.get::<_, i64>(0)
        )?;

        Ok(count > 0)
    }

    #[allow(dead_code)]
    pub fn insert_image(&self, image: Image) -> SqlResult<()> {
        self.connection.execute("INSERT INTO images (tag, hash) VALUES (?1, ?2)", (&image.tag, &image.hash))?;
        Ok(())
    }

    pub fn insert_or_replace_image(&mut self, image: Image) -> ImageManagerResult<()> {
        self.connection.execute("REPLACE INTO images (tag, hash) VALUES (?1, ?2)", (&image.tag, &image.hash))?;
        Ok(())
    }

    pub fn remove_image(&mut self, tag: &ImageTag) -> SqlResult<Option<Image>> {
        let transaction = self.connection.transaction()?;
        let image = StateSession::get_image_internal(&transaction, tag)?;
        transaction.execute("DELETE FROM images WHERE tag=?1", (&tag, ))?;
        transaction.commit()?;
        Ok(image)
    }

    pub fn all_unpackings(&self) -> SqlResult<Vec<Unpacking>> {
        let mut statement = self.connection.prepare("SELECT destination, hash, time FROM unpackings")?;

        let mut unpackings = Vec::new();
        for image in statement.query_map([], |row| Unpacking::from_row(&row))? {
            unpackings.push(image?);
        }

        Ok(unpackings)
    }

    pub fn get_unpacking(&self, destination: &str) -> SqlResult<Option<Unpacking>> {
        self.connection.query_row(
            "SELECT destination, hash, time FROM unpackings WHERE destination=?1",
            [destination],
            |row| Unpacking::from_row(&row)
        ).optional()
    }

    pub fn unpacking_exist_at(&self, destination: &str) -> SqlResult<bool> {
        let count = self.connection.query_one(
            "SELECT COUNT(*) FROM unpackings WHERE destination=?1",
            [destination],
            |row| row.get::<_, i64>(0)
        )?;

        Ok(count > 0)
    }

    pub fn insert_unpacking(&self, unpacking: Unpacking) -> SqlResult<()> {
        self.connection.execute(
            "INSERT INTO unpackings (destination, hash, time) VALUES (?1, ?2, ?3)",
            (&unpacking.destination, &unpacking.hash, &unpacking.time)
        )?;
        Ok(())
    }

    pub fn remove_unpacking(&self, destination: &str) -> SqlResult<()> {
        self.connection.execute("DELETE FROM unpackings WHERE destination=?1", (&destination, ))?;
        Ok(())
    }

    pub fn get_content_hash(&self, file: &str, modified: u64) -> SqlResult<Option<String>> {
        self.connection.query_row(
            "SELECT hash FROM content_hash_cache WHERE file=?1 AND modified=?2",
            (file, modified),
            |row| row.get(0)
        ).optional()
    }

    pub fn insert_content_hash(&self, file: &str, modified: u64, hash: &str) -> SqlResult<()> {
        StateSession::insert_content_hash_internal(&self.connection, file, modified, hash)?;
        Ok(())
    }

    pub fn insert_content_hashes(&mut self, hashes: Vec<(String, u64, String)>) -> SqlResult<()> {
        let transaction = self.connection.transaction()?;

        for (file, modified, hash) in hashes {
            StateSession::insert_content_hash_internal(&transaction, &file, modified, &hash)?;
        }

        transaction.commit()?;
        Ok(())
    }

    fn insert_content_hash_internal(connection: &Connection, file: &str, modified: u64, hash: &str) -> SqlResult<()> {
        connection.execute("REPLACE INTO content_hash_cache (file, modified, hash) VALUES (?1, ?2, ?3)", (file, modified, hash))?;
        Ok(())
    }

    pub fn registry_try_start_layer_upload(&mut self,
                                           current_time: DateTime<Local>,
                                           layer: &Layer,
                                           upload_id: &str,
                                           pending_upload_expiration: f64) -> SqlResult<bool> {
        let transaction = self.connection.transaction()?;

        let current_pending_upload_last_update: Option<DateTime<Local>> = transaction.query_row(
            "SELECT last_updated FROM registry_pending_layer_uploads WHERE hash=?1",
            [layer.hash.to_string()],
            |row| row.get(0)
        ).optional()?;

        if let Some(current_pending_upload_last_update) = current_pending_upload_last_update {
            if (current_time - current_pending_upload_last_update).to_std().unwrap().as_secs_f64() < pending_upload_expiration {
                return Ok(false);
            }
        }

        transaction.execute(
            "INSERT INTO registry_pending_layer_uploads (hash, layer_metadata, last_updated, upload_id, state) VALUES (?1, ?2, ?3, ?4, ?5)",
            (&layer.hash, &serde_json::to_value(&layer).unwrap(), current_time, upload_id, "uploading")
        )?;

        transaction.commit()?;
        Ok(true)
    }

    pub fn registry_get_layer_upload_by_id(&self, upload_id: &str) -> SqlResult<Option<Layer>> {
        self.connection.query_row(
            "SELECT layer_metadata FROM registry_pending_layer_uploads WHERE upload_id=?1",
            [upload_id],
            |row| row.get(0)
        ).optional()
    }

    pub fn registry_end_layer_upload(&mut self, layer: Layer) -> SqlResult<bool> {
        let transaction = self.connection.transaction()?;

        let count = transaction.execute("DELETE FROM registry_pending_layer_uploads WHERE hash=?1", (&layer.hash, ))?;
        if count != 1 {
            return Ok(false);
        }

        StateSession::insert_layer_internal(&transaction, &layer)?;

        transaction.commit()?;
        Ok(true)
    }

    pub fn registry_remove_upload(&mut self, upload_id: &str) -> SqlResult<bool> {
        let count = self.connection.execute("DELETE FROM registry_pending_layer_uploads WHERE upload_id=?1", (&upload_id, ))?;
        Ok(count != 1)
    }
}

pub struct PooledStateSession {
    pool: Arc<StateSessionPool>,
    session: Option<StateSession>
}

impl PooledStateSession {
    fn new(pool: Arc<StateSessionPool>, session: StateSession) -> PooledStateSession {
        PooledStateSession {
            pool,
            session: Some(session)
        }
    }
}

impl Drop for PooledStateSession {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            self.pool.return_session(session);
        }
    }
}

impl Deref for PooledStateSession {
    type Target = StateSession;

    fn deref(&self) -> &Self::Target {
        self.session.as_ref().unwrap()
    }
}

impl DerefMut for PooledStateSession {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.session.as_mut().unwrap()
    }
}

struct StateSessionPool {
    sessions: Mutex<Vec<StateSession>>
}

impl StateSessionPool {
    pub fn new(initial: Vec<StateSession>) -> StateSessionPool {
        StateSessionPool {
            sessions: Mutex::new(initial)
        }
    }

    pub fn return_session(&self, session: StateSession) {
        self.sessions.lock().unwrap().push(session);
    }

    pub fn get_session(&self) -> Option<StateSession> {
        self.sessions.lock().unwrap().pop()
    }
}