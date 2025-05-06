use entity::user::{ActiveModel as UserActiveModel, Model as UserModel, Entity as User};
use entity::sea_orm::{ActiveValue, DatabaseConnection, EntityTrait, ColumnTrait, QueryFilter, ActiveModelTrait};
use argon2::{Argon2, Algorithm, Version, Params, password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString}};
use secrecy::{ExposeSecret, SecretString};
use anyhow::{Result, Context};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

struct PasswordManager<'key> {
    argon2: Argon2<'key>,
}

impl<'key> PasswordManager<'key> {
    fn new() -> Result<Self> {
        let params = Params::new(19_456u32, 2u32, 1u32, None)?;
        let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
        Ok(Self { argon2  })
    }

    fn hash_password(self, password: &SecretString) -> Result<String> {
        let salt = SaltString::generate(&mut OsRng);
        let key = self.argon2.hash_password(password.expose_secret().as_bytes(), &salt)?.to_string();
        tracing::debug!("Hashed password len: {}", key.len());
        Ok(key)
    }

    fn verify_password(self, password: &SecretString, hash: &str) -> Result<()> {
        let hash = PasswordHash::new(hash)?;
        self.argon2.verify_password(password.expose_secret().as_bytes(), &hash)?;
        Ok(())
    }
}

#[derive(Debug, Deserialize)]
pub struct CreateUser {
    username: String,
    password: SecretString,
    is_superuser: bool,
}

impl CreateUser {
    pub fn new(username: &str, password: &str, is_superuser: bool) -> Self {
        Self { username: username.to_string(), password: SecretString::new(password.to_string().into()), is_superuser }
    }

    pub fn to_model(self) -> Result<UserActiveModel> {
        let password_manager = PasswordManager::new()?;
        let password = password_manager.hash_password(&self.password)?;
        let user = UserActiveModel {
            id: ActiveValue::Set(Uuid::new_v4()),
            username: ActiveValue::Set(self.username.clone()),
            password: ActiveValue::Set(Some(password)),
            is_superuser: ActiveValue::Set(self.is_superuser),
        };
        Ok(user)
    }
}

pub struct VerifyUser {
    username: String,
    password: SecretString,
}

impl VerifyUser {
    pub fn new(username: &str, password: &str) -> Self {
        Self { username: username.to_string(), password: SecretString::new(password.to_string().into()) }
    }

    pub fn verify(self, user: &UserModel) -> Result<()> {
        if user.username != self.username {
            return Err(anyhow::anyhow!("Username does not match"));
        }
        let password_manager = PasswordManager::new()?;
        if let Some(hash) = &user.password {
            password_manager.verify_password(&self.password, hash.as_str())
        } else {
            Err(anyhow::anyhow!("User is disabled"))
        }
    }
}

#[derive(Debug, Serialize)]
pub struct UserResponse {
    pub id: Uuid,
    pub username: String,
    pub is_superuser: bool,
}

impl From<UserModel> for UserResponse {
    fn from(user: UserModel) -> Self {
        Self {
            id: user.id,
            username: user.username,
            is_superuser: user.is_superuser,
        }
    }
}

pub struct UserService<'db> {
    db: &'db DatabaseConnection,
}

impl<'db> UserService<'db> {
    pub fn new(db: &'db DatabaseConnection) -> Self {
        Self { db }
    }

    pub async fn create_user(&self, create_user: CreateUser) -> Result<UserResponse> {
        let user_model = create_user.to_model()?;
        let user = user_model.insert(self.db)
            .await
            .context("Failed to insert user")?;
        Ok(user.into())
    }

    pub async fn find_by_username(&self, username: &str) -> Result<Option<UserModel>> {
        User::find()
            .filter(entity::user::Column::Username.eq(username))
            .one(self.db)
            .await
            .context("Failed to query user")
    }

    pub async fn find_by_id(&self, id: Uuid) -> Result<Option<UserModel>> {
        User::find_by_id(id)
            .one(self.db)
            .await
            .context("Failed to query user")
    }

    pub async fn verify_user(&self, username: &str, password: &str) -> Result<UserResponse> {
        let user = self.find_by_username(username)
            .await?
            .ok_or_else(|| anyhow::anyhow!("User not found"))?;

        let verify = VerifyUser::new(username, password);
        verify.verify(&user)?;
        
        Ok(user.into())
    }
}
