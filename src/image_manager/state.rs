use std::path::Path;
use rusqlite::{Connection, OptionalExtension};

use crate::image::{Image, Layer};
use crate::image_manager::unpack::Unpacking;
use crate::reference::{ImageId, ImageTag};

pub type SqlResult<T> = rusqlite::Result<T>;

pub struct StateManager {
    pub connection: Connection
}

impl StateManager {
    pub fn new(base_folder: &Path) -> SqlResult<StateManager> {
        if !base_folder.exists() {
            std::fs::create_dir_all(base_folder).map_err(|_| rusqlite::Error::InvalidPath(base_folder.to_path_buf()))?;
        }

        let connection = Connection::open(base_folder.join("state.sqlite3"))?;
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

        Ok(
            StateManager {
                connection,
            }
        )
    }

    pub fn begin_transaction(&self) -> SqlResult<()> {
        self.connection.execute("BEGIN TRANSACTION", ())?;
        Ok(())
    }

    pub fn end_transaction(&self) -> SqlResult<()> {
        self.connection.execute("COMMIT", ())?;
        Ok(())
    }

    pub fn add_login(&self, registry: &str, username: &str, password: &str) -> SqlResult<()> {
        self.begin_transaction()?;
        self.connection.execute("DELETE FROM logins WHERE registry=?1", (registry, ))?;
        self.connection.execute("INSERT INTO logins (registry, username, password) VALUES (?1, ?2, ?3)", (registry, username, password))?;
        self.end_transaction()?;
        Ok(())
    }

    pub fn get_login(&self, registry: &str) -> SqlResult<Option<(String, String)>> {
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

    pub fn get_layer(&self, hash: &ImageId) -> SqlResult<Option<Layer>> {
        self.connection.query_row(
            "SELECT metadata FROM layers WHERE hash=?1",
            [hash.to_string()],
            |row| row.get(0)
        ).optional()
    }

    pub fn insert_layer(&self, layer: Layer) -> SqlResult<()> {
        self.connection.execute("INSERT INTO layers (hash, metadata) VALUES (?1, ?2)", (&layer.hash, &serde_json::to_value(&layer).unwrap()))?;
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
        self.connection.query_row(
            "SELECT hash, tag FROM images WHERE tag=?1",
            [tag],
            |row| Image::from_row(&row)
        ).optional()
    }

    pub fn image_exists(&self, tag: &ImageTag) -> SqlResult<bool> {
        let count = self.connection.query_one(
            "SELECT COUNT(*) FROM images WHERE tag=?1",
            [tag],
            |row| row.get::<_, i64>(0)
        )?;

        Ok(count > 0)
    }

    pub fn insert_image(&self, image: Image) -> SqlResult<()> {
        self.connection.execute("INSERT INTO images (tag, hash) VALUES (?1, ?2)", (&image.tag, &image.hash))?;
        Ok(())
    }

    pub fn replace_image(&self, image: Image) -> SqlResult<()> {
        self.connection.execute("UPDATE images SET hash=?2 WHERE tag=?1", (&image.tag, &image.hash))?;
        Ok(())
    }

    pub fn remove_image(&self, tag: &ImageTag) -> SqlResult<()> {
        self.connection.execute("DELETE FROM images WHERE tag=?1", (&tag, ))?;
        Ok(())
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
        self.connection.execute("INSERT INTO unpackings (destination, hash, time) VALUES (?1, ?2, ?3)", (&unpacking.destination, &unpacking.hash, &unpacking.time))?;
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

    pub fn add_content_hash(&self, file: &str, modified: u64, hash: &str) -> SqlResult<()> {
        self.connection.execute("INSERT INTO content_hash_cache (file, modified, hash) VALUES (?1, ?2, ?3)", (file, modified, hash))?;
        Ok(())
    }
}