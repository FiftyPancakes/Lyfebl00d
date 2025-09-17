//! Secure Secrets Management System
//! 
//! This module provides a production-grade secrets management system for the DeFi arbitrage bot.
//! It uses industry-standard encryption and follows security best practices for handling sensitive data.

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use argon2::{Argon2, PasswordHash, PasswordHasher, PasswordVerifier, password_hash::SaltString};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
use tracing::{info, warn, error, debug};
use zeroize::{Zeroize, ZeroizeOnDrop};
use eyre::{Result, eyre};

/// Categories of secrets for role-based access control
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SecretCategory {
    /// Private keys for transaction signing
    PrivateKeys,
    /// External API keys (CoinGecko, 1inch, etc.)
    ApiKeys,
    /// Database connection credentials
    Database,
    /// RPC endpoint authentication
    RpcAuth,
    /// Cross-chain bridge credentials
    BridgeAuth,
    /// MEV protection service credentials
    MevProtection,
    /// Monitoring and metrics credentials
    Monitoring,
    /// Development/testing secrets (separate vault)
    Development,
}

/// Sensitive configuration data that needs encryption
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecretData {
    /// Private keys for different chains
    pub private_keys: HashMap<String, String>, // chain_name -> private_key
    
    /// API keys for external services
    pub api_keys: HashMap<String, String>, // service_name -> api_key
    
    /// Database credentials
    pub database: DatabaseSecrets,
    
    /// RPC authentication tokens
    pub rpc_auth: HashMap<String, RpcCredentials>, // chain_name -> credentials
    
    /// Bridge service credentials
    pub bridge_auth: HashMap<String, BridgeCredentials>, // bridge_name -> credentials
    
    /// MEV protection service credentials
    pub mev_protection: MevProtectionSecrets,
    
    /// Monitoring service credentials
    pub monitoring: MonitoringSecrets,
}

impl Zeroize for SecretData {
    fn zeroize(&mut self) {
        for (_, value) in self.private_keys.iter_mut() {
            value.zeroize();
        }
        self.private_keys.clear();
        
        for (_, value) in self.api_keys.iter_mut() {
            value.zeroize();
        }
        self.api_keys.clear();
        
        self.database.zeroize();
        
        for (_, value) in self.rpc_auth.iter_mut() {
            value.zeroize();
        }
        self.rpc_auth.clear();
        
        for (_, value) in self.bridge_auth.iter_mut() {
            value.zeroize();
        }
        self.bridge_auth.clear();
        
        self.mev_protection.zeroize();
        self.monitoring.zeroize();
    }
}

impl ZeroizeOnDrop for SecretData {}

#[derive(Debug, Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct DatabaseSecrets {
    pub postgres: Option<PostgresCredentials>,
    pub redis: Option<RedisCredentials>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct PostgresCredentials {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
    pub ssl_cert_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct RedisCredentials {
    pub host: String,
    pub port: u16,
    pub password: Option<String>,
    pub username: Option<String>,
    pub tls_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct RpcCredentials {
    pub api_key: Option<String>,
    pub bearer_token: Option<String>,
    pub basic_auth: Option<BasicAuth>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct BasicAuth {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct BridgeCredentials {
    pub api_key: String,
    pub secret_key: Option<String>,
    pub webhook_secret: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct MevProtectionSecrets {
    pub flashbots_signing_key: Option<String>,
    pub eden_api_key: Option<String>,
    pub bloxroute_auth_header: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Zeroize, ZeroizeOnDrop)]
pub struct MonitoringSecrets {
    pub prometheus_basic_auth: Option<BasicAuth>,
    pub grafana_api_key: Option<String>,
    pub discord_webhook: Option<String>,
    pub telegram_bot_token: Option<String>,
}

/// Encrypted vault structure stored on disk
#[derive(Debug, Serialize, Deserialize)]
struct EncryptedVault {
    /// Vault metadata
    metadata: VaultMetadata,
    /// Encrypted secret data
    encrypted_data: Vec<u8>,
    /// Nonce used for encryption
    nonce: Vec<u8>,
    /// Salt used for key derivation
    salt: Vec<u8>,
    /// Password hash for verification
    password_hash: String,
    /// Vault format version for future compatibility
    version: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct VaultMetadata {
    /// Environment this vault is for (dev, staging, prod)
    environment: String,
    /// Timestamp when vault was created
    created_at: u64,
    /// Timestamp when vault was last modified
    modified_at: u64,
    /// Categories of secrets stored in this vault
    categories: Vec<SecretCategory>,
    /// Checksum for integrity verification
    checksum: String,
}

/// Audit log entry for secret access
#[derive(Debug, Serialize, Deserialize)]
pub struct AuditLogEntry {
    pub timestamp: u64,
    pub operation: String,
    pub category: Option<SecretCategory>,
    pub secret_key: Option<String>,
    pub success: bool,
    pub error: Option<String>,
    pub process_id: u32,
    pub environment: String,
}

/// Main secrets manager
pub struct SecretsManager {
    /// Current environment (dev, staging, prod)
    environment: String,
    /// Path to secrets directory
    secrets_dir: PathBuf,
    /// Cached decrypted secrets
    secrets_cache: Option<SecretData>,
    /// Audit logger
    audit_logger: AuditLogger,
    /// Security configuration
    security_config: SecurityConfig,
}

#[derive(Debug, Clone)]
pub struct SecurityConfig {
    /// Maximum time to keep secrets in memory (seconds)
    pub max_cache_time: u64,
    /// Enable audit logging
    pub enable_audit_logging: bool,
    /// Require password confirmation for sensitive operations
    pub require_password_confirmation: bool,
    /// Automatically clear cache on inactivity
    pub auto_clear_cache: bool,
    /// Argon2 parameters for key derivation
    pub argon2_params: Argon2Params,
}

#[derive(Debug, Clone)]
pub struct Argon2Params {
    pub memory_cost: u32,      // Memory cost in KB
    pub time_cost: u32,        // Time cost (iterations)
    pub parallelism: u32,      // Parallelism factor
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            max_cache_time: 3600, // 1 hour
            enable_audit_logging: true,
            require_password_confirmation: true,
            auto_clear_cache: true,
            argon2_params: Argon2Params {
                memory_cost: 65536,    // 64 MB
                time_cost: 3,          // 3 iterations
                parallelism: 4,        // 4 parallel threads
            },
        }
    }
}

impl Default for SecretData {
    fn default() -> Self {
        Self {
            private_keys: HashMap::new(),
            api_keys: HashMap::new(),
            database: DatabaseSecrets {
                postgres: None,
                redis: None,
            },
            rpc_auth: HashMap::new(),
            bridge_auth: HashMap::new(),
            mev_protection: MevProtectionSecrets {
                flashbots_signing_key: None,
                eden_api_key: None,
                bloxroute_auth_header: None,
            },
            monitoring: MonitoringSecrets {
                prometheus_basic_auth: None,
                grafana_api_key: None,
                discord_webhook: None,
                telegram_bot_token: None,
            },
        }
    }
}

/// Audit logger for tracking secret access
pub struct AuditLogger {
    log_file: PathBuf,
    enabled: bool,
}

impl AuditLogger {
    pub fn new(secrets_dir: &Path, enabled: bool) -> Self {
        let log_file = secrets_dir.join("audit.log");
        Self { log_file, enabled }
    }

    pub async fn log(&self, entry: AuditLogEntry) {
        if !self.enabled {
            return;
        }

        let log_line = match serde_json::to_string(&entry) {
            Ok(json) => format!("{}\n", json),
            Err(e) => {
                error!("Failed to serialize audit log entry: {}", e);
                return;
            }
        };

        let file_result = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_file)
            .await;
            
        if let Ok(mut file) = file_result {
            use tokio::io::AsyncWriteExt;
            if let Err(e) = file.write_all(log_line.as_bytes()).await {
                error!("Failed to write audit log: {}", e);
            }
        } else {
            error!("Failed to open audit log file");
        }
    }
}

impl SecretsManager {
    /// Create a new secrets manager
    pub fn new(environment: String, secrets_dir: PathBuf) -> Result<Self> {
        let security_config = SecurityConfig::default();
        
        // Ensure secrets directory exists with proper permissions
        if !secrets_dir.exists() {
            fs::create_dir_all(&secrets_dir)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let metadata = fs::metadata(&secrets_dir)?;
                let mut permissions = metadata.permissions();
                permissions.set_mode(0o700); // rwx------
                fs::set_permissions(&secrets_dir, permissions)?;
            }
        }

        let audit_logger = AuditLogger::new(&secrets_dir, security_config.enable_audit_logging);

        Ok(Self {
            environment,
            secrets_dir,
            secrets_cache: None,
            audit_logger,
            security_config,
        })
    }

    /// Initialize a new vault with a master password
    pub async fn initialize_vault(&mut self, master_password: &str) -> Result<()> {
        let vault_path = self.get_vault_path();
        
        if vault_path.exists() {
            return Err(eyre!("Vault already exists for environment: {}", self.environment));
        }

        info!("Initializing new secrets vault for environment: {}", self.environment);

        let secret_data = SecretData::default();
        self.save_vault(&secret_data, master_password).await?;

        self.log_audit("vault_initialized", None, None, true, None).await;
        Ok(())
    }

    /// Load and decrypt the vault
    pub async fn load_vault(&mut self, master_password: &str) -> Result<()> {
        let vault_path = self.get_vault_path();
        
        if !vault_path.exists() {
            return Err(eyre!("No vault found for environment: {}", self.environment));
        }

        debug!("Loading secrets vault from: {}", vault_path.display());

        let vault_data = fs::read(&vault_path)?;
        let encrypted_vault: EncryptedVault = serde_json::from_slice(&vault_data)?;

        // Verify password
        let parsed_hash = PasswordHash::new(&encrypted_vault.password_hash)
            .map_err(|e| eyre!("Invalid password hash format: {}", e))?;
        if Argon2::default().verify_password(master_password.as_bytes(), &parsed_hash).is_err() {
            self.log_audit("vault_load_failed", None, None, false, Some("Invalid password")).await;
            return Err(eyre!("Invalid master password"));
        }

        // Derive decryption key
        let key = self.derive_key(master_password, &encrypted_vault.salt)?;
        
        // Decrypt data
        let cipher = Aes256Gcm::new(&key);
        let nonce = Nonce::from_slice(&encrypted_vault.nonce);
        
        let decrypted_data = cipher.decrypt(nonce, encrypted_vault.encrypted_data.as_ref())
            .map_err(|e| eyre!("Failed to decrypt vault: {}", e))?;

        let secret_data: SecretData = serde_json::from_slice(&decrypted_data)?;
        
        self.secrets_cache = Some(secret_data);
        self.log_audit("vault_loaded", None, None, true, None).await;

        info!("Successfully loaded secrets vault for environment: {}", self.environment);
        Ok(())
    }

    /// Save the vault with encryption
    async fn save_vault(&self, secret_data: &SecretData, master_password: &str) -> Result<()> {
        let vault_path = self.get_vault_path();

        // Generate salt for key derivation
        let salt = SaltString::generate(&mut OsRng);
        
        // Derive encryption key
        let key = self.derive_key(master_password, salt.as_salt().as_str().as_bytes())?;
        
        // Generate password hash for verification
        let argon2 = Argon2::default();
        let password_hash = argon2.hash_password(master_password.as_bytes(), &salt)
            .map_err(|e| eyre!("Failed to hash password: {}", e))?
            .to_string();

        // Serialize secret data
        let plaintext = serde_json::to_vec(secret_data)?;

        // Encrypt data
        let cipher = Aes256Gcm::new(&key);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let encrypted_data = cipher.encrypt(&nonce, plaintext.as_ref())
            .map_err(|e| eyre!("Encryption failed: {}", e))?;

        // Create vault structure
        let vault = EncryptedVault {
            metadata: VaultMetadata {
                environment: self.environment.clone(),
                created_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                modified_at: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                categories: vec![
                    SecretCategory::PrivateKeys,
                    SecretCategory::ApiKeys,
                    SecretCategory::Database,
                    SecretCategory::RpcAuth,
                    SecretCategory::BridgeAuth,
                    SecretCategory::MevProtection,
                    SecretCategory::Monitoring,
                ],
                checksum: format!("{:x}", md5::compute(&encrypted_data)),
            },
            encrypted_data,
            nonce: nonce.to_vec(),
            salt: salt.as_salt().as_str().as_bytes().to_vec(),
            password_hash,
            version: 1,
        };

        // Write to disk with secure permissions
        let vault_json = serde_json::to_string_pretty(&vault)?;
        fs::write(&vault_path, vault_json)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(&vault_path)?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o600); // rw-------
            fs::set_permissions(&vault_path, permissions)?;
        }

        debug!("Saved encrypted vault to: {}", vault_path.display());
        Ok(())
    }

    /// Get a private key for a specific chain
    pub async fn get_private_key(&self, chain_name: &str) -> Result<Option<String>> {
        if self.secrets_cache.is_none() {
            return Err(eyre!("Vault not loaded. Call load_vault() first."));
        }

        let key = self.secrets_cache.as_ref().unwrap()
            .private_keys.get(chain_name).cloned();

        self.log_audit(
            "get_private_key", 
            Some(SecretCategory::PrivateKeys), 
            Some(chain_name.to_string()), 
            true, 
            None
        ).await;

        Ok(key)
    }

    /// Set a private key for a specific chain
    pub async fn set_private_key(&mut self, chain_name: &str, private_key: &str, master_password: &str) -> Result<()> {
        if self.secrets_cache.is_none() {
            return Err(eyre!("Vault not loaded. Call load_vault() first."));
        }

        let mut secret_data = self.secrets_cache.as_ref().unwrap().clone();
        secret_data.private_keys.insert(chain_name.to_string(), private_key.to_string());

        self.save_vault(&secret_data, master_password).await?;
        self.secrets_cache = Some(secret_data);

        self.log_audit(
            "set_private_key", 
            Some(SecretCategory::PrivateKeys), 
            Some(chain_name.to_string()), 
            true, 
            None
        ).await;

        Ok(())
    }

    /// Get an API key for a specific service
    pub async fn get_api_key(&self, service_name: &str) -> Result<Option<String>> {
        if self.secrets_cache.is_none() {
            return Err(eyre!("Vault not loaded. Call load_vault() first."));
        }

        let key = self.secrets_cache.as_ref().unwrap()
            .api_keys.get(service_name).cloned();

        self.log_audit(
            "get_api_key", 
            Some(SecretCategory::ApiKeys), 
            Some(service_name.to_string()), 
            true, 
            None
        ).await;

        Ok(key)
    }

    /// Set an API key for a specific service
    pub async fn set_api_key(&mut self, service_name: &str, api_key: &str, master_password: &str) -> Result<()> {
        if self.secrets_cache.is_none() {
            return Err(eyre!("Vault not loaded. Call load_vault() first."));
        }

        let mut secret_data = self.secrets_cache.as_ref().unwrap().clone();
        secret_data.api_keys.insert(service_name.to_string(), api_key.to_string());

        self.save_vault(&secret_data, master_password).await?;
        self.secrets_cache = Some(secret_data);

        self.log_audit(
            "set_api_key", 
            Some(SecretCategory::ApiKeys), 
            Some(service_name.to_string()), 
            true, 
            None
        ).await;

        Ok(())
    }

    /// Get database credentials
    pub async fn get_database_credentials(&self) -> Result<DatabaseSecrets> {
        if self.secrets_cache.is_none() {
            return Err(eyre!("Vault not loaded. Call load_vault() first."));
        }

        let db_secrets = self.secrets_cache.as_ref().unwrap().database.clone();

        self.log_audit(
            "get_database_credentials", 
            Some(SecretCategory::Database), 
            None, 
            true, 
            None
        ).await;

        Ok(db_secrets)
    }

    /// Set database credentials
    pub async fn set_database_credentials(&mut self, db_secrets: DatabaseSecrets, master_password: &str) -> Result<()> {
        if self.secrets_cache.is_none() {
            return Err(eyre!("Vault not loaded. Call load_vault() first."));
        }

        let mut secret_data = self.secrets_cache.as_ref().unwrap().clone();
        secret_data.database = db_secrets;

        self.save_vault(&secret_data, master_password).await?;
        self.secrets_cache = Some(secret_data);

        self.log_audit(
            "set_database_credentials", 
            Some(SecretCategory::Database), 
            None, 
            true, 
            None
        ).await;

        Ok(())
    }

    /// Clear cached secrets from memory
    pub async fn clear_cache(&mut self) {
        if let Some(mut secrets) = self.secrets_cache.take() {
            secrets.zeroize();
            self.log_audit("cache_cleared", None, None, true, None).await;
            info!("Cleared secrets cache from memory");
        }
    }

    /// Check if environment variable override exists
    pub fn get_env_override(&self, key: &str) -> Option<String> {
        let env_key = format!("{}_{}", self.environment.to_uppercase(), key.to_uppercase());
        std::env::var(&env_key).ok()
    }

    /// Derive encryption key from password and salt
    fn derive_key(&self, password: &str, salt: &[u8]) -> Result<Key<Aes256Gcm>> {
        let params = &self.security_config.argon2_params;
        let argon2 = Argon2::new(
            argon2::Algorithm::Argon2id,
            argon2::Version::V0x13,
            argon2::Params::new(
                params.memory_cost,
                params.time_cost,
                params.parallelism,
                Some(32), // Output length for AES-256
            ).map_err(|e| eyre!("Failed to create Argon2 params: {}", e))?,
        );

        let mut key_bytes = [0u8; 32];
        argon2.hash_password_into(password.as_bytes(), salt, &mut key_bytes)
            .map_err(|e| eyre!("Failed to derive key: {}", e))?;
        
        Ok(*Key::<Aes256Gcm>::from_slice(&key_bytes))
    }

    /// Get path to vault file
    fn get_vault_path(&self) -> PathBuf {
        self.secrets_dir.join(format!("{}.vault", self.environment))
    }

    /// Log audit entry
    async fn log_audit(
        &self,
        operation: &str,
        category: Option<SecretCategory>,
        secret_key: Option<String>,
        success: bool,
        error: Option<&str>,
    ) {
        let entry = AuditLogEntry {
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)
                .unwrap_or_default().as_secs(),
            operation: operation.to_string(),
            category,
            secret_key,
            success,
            error: error.map(|e| e.to_string()),
            process_id: std::process::id(),
            environment: self.environment.clone(),
        };

        self.audit_logger.log(entry).await;
    }

    /// Backup vault to a secure location
    pub async fn backup_vault(&self, backup_path: &Path) -> Result<()> {
        let vault_path = self.get_vault_path();
        
        if !vault_path.exists() {
            return Err(eyre!("No vault found to backup"));
        }

        fs::copy(&vault_path, backup_path)?;
        
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(backup_path)?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o600); // rw-------
            fs::set_permissions(backup_path, permissions)?;
        }

        self.log_audit("vault_backup", None, None, true, None).await;
        info!("Vault backed up to: {}", backup_path.display());
        Ok(())
    }

    /// Restore vault from backup
    pub async fn restore_vault(&mut self, backup_path: &Path, master_password: &str) -> Result<()> {
        if !backup_path.exists() {
            return Err(eyre!("Backup file not found: {}", backup_path.display()));
        }

        let vault_path = self.get_vault_path();
        fs::copy(backup_path, &vault_path)?;

        // Verify the restored vault by loading it
        self.load_vault(master_password).await?;

        self.log_audit("vault_restored", None, None, true, None).await;
        info!("Vault restored from: {}", backup_path.display());
        Ok(())
    }

    /// Rotate master password
    pub async fn rotate_master_password(&mut self, old_password: &str, new_password: &str) -> Result<()> {
        // Load with old password
        self.load_vault(old_password).await?;
        
        if let Some(secret_data) = &self.secrets_cache {
            // Save with new password
            self.save_vault(secret_data, new_password).await?;
            self.log_audit("password_rotated", None, None, true, None).await;
            info!("Master password rotated successfully");
        } else {
            return Err(eyre!("No secrets loaded for password rotation"));
        }

        Ok(())
    }

    /// Export secrets to a temporary file for migration (use with caution)
    pub async fn export_secrets(&self, export_path: &Path, master_password: &str) -> Result<()> {
        if self.secrets_cache.is_none() {
            return Err(eyre!("Vault not loaded. Call load_vault() first."));
        }

        // Verify master password before allowing export
        let vault_path = self.get_vault_path();
        if vault_path.exists() {
            let vault_data = fs::read(&vault_path)?;
            let encrypted_vault: EncryptedVault = serde_json::from_slice(&vault_data)?;

            // Verify password using the same method as load_vault
            let parsed_hash = PasswordHash::new(&encrypted_vault.password_hash)
                .map_err(|e| eyre!("Invalid password hash format: {}", e))?;
            if Argon2::default().verify_password(master_password.as_bytes(), &parsed_hash).is_err() {
                self.log_audit("export_failed", None, None, false, Some("Invalid password")).await;
                return Err(eyre!("Invalid master password for export operation"));
            }
        }

        warn!("SECURITY WARNING: Exporting secrets to plaintext file: {}", export_path.display());

        let secret_data = self.secrets_cache.as_ref().unwrap();
        let json = serde_json::to_string_pretty(secret_data)?;

        fs::write(export_path, json)?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(export_path)?;
            let mut permissions = metadata.permissions();
            permissions.set_mode(0o600); // rw-------
            fs::set_permissions(export_path, permissions)?;
        }

        self.log_audit("secrets_exported", None, None, true, None).await;
        warn!("Secrets exported. Remember to securely delete the export file when done.");
        Ok(())
    }

    /// Get all secrets data (for config integration)
    pub async fn get_all_secrets(&self) -> Result<SecretData> {
        if self.secrets_cache.is_none() {
            return Err(eyre!("Vault not loaded. Call load_vault() first."));
        }

        Ok(self.secrets_cache.as_ref().unwrap().clone())
    }
}

/// Helper function to get secrets manager instance
pub async fn get_secrets_manager(environment: &str) -> Result<SecretsManager> {
    let secrets_dir = PathBuf::from("secrets");
    SecretsManager::new(environment.to_string(), secrets_dir)
}

/// Helper function to load secrets for a specific environment
pub async fn load_secrets(environment: &str, master_password: &str) -> Result<SecretsManager> {
    let mut manager = get_secrets_manager(environment).await?;
    manager.load_vault(master_password).await?;
    Ok(manager)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_vault_lifecycle() {
        let temp_dir = TempDir::new().unwrap();
        let secrets_dir = temp_dir.path().to_path_buf();
        
        let mut manager = SecretsManager::new("test".to_string(), secrets_dir).unwrap();
        
        // Initialize vault
        manager.initialize_vault("test_password").await.unwrap();
        
        // Load vault
        manager.load_vault("test_password").await.unwrap();
        
        // Set and get API key
        manager.set_api_key("coingecko", "test_api_key", "test_password").await.unwrap();
        let api_key = manager.get_api_key("coingecko").await.unwrap();
        assert_eq!(api_key, Some("test_api_key".to_string()));
        
        // Clear cache
        manager.clear_cache().await;
        
        // Reload and verify persistence
        manager.load_vault("test_password").await.unwrap();
        let api_key = manager.get_api_key("coingecko").await.unwrap();
        assert_eq!(api_key, Some("test_api_key".to_string()));
    }

    #[tokio::test]
    async fn test_invalid_password() {
        let temp_dir = TempDir::new().unwrap();
        let secrets_dir = temp_dir.path().to_path_buf();
        
        let mut manager = SecretsManager::new("test".to_string(), secrets_dir).unwrap();
        
        // Initialize vault
        manager.initialize_vault("correct_password").await.unwrap();
        
        // Try to load with wrong password
        let result = manager.load_vault("wrong_password").await;
        assert!(result.is_err());
    }
} 