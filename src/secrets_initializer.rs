use clap::{Parser, Subcommand};
use std::path::PathBuf;
use eyre::{Result, eyre};
use tracing::{info, error};
use rust::secrets::{get_secrets_manager, SecretsManager};
use rpassword::read_password;

#[derive(Parser)]
#[command(name = "secrets_initializer")]
#[command(about = "Initialize and manage encrypted secrets for the arbitrage bot")]
struct Args {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize secrets storage with a new master password
    Init {
        /// Environment (e.g., production, development)
        #[arg(short, long, default_value = "production")]
        environment: String,
    },
    /// Add a new secret to the vault
    Add {
        /// Environment (e.g., production, development)
        #[arg(short, long, default_value = "production")]
        environment: String,
        /// Name/key of the secret
        #[arg(short, long)]
        key: String,
        /// Value of the secret
        #[arg(short, long)]
        value: String,
    },
    /// Retrieve a secret from the vault
    Get {
        /// Environment (e.g., production, development)
        #[arg(short, long, default_value = "production")]
        environment: String,
        /// Name/key of the secret
        #[arg(short, long)]
        key: String,
    },
    /// List all keys in the vault
    List {
        /// Environment (e.g., production, development)
        #[arg(short, long, default_value = "production")]
        environment: String,
    },
    /// Change the master password
    ChangePassword {
        /// Environment (e.g., production, development)
        #[arg(short, long, default_value = "production")]
        environment: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let args = Args::parse();

    match args.command {
        Commands::Init { environment } => {
            info!("Initializing secrets for environment: {}", environment);
            println!("Enter master password:");
            let master_password = read_password()?;

            let mut manager = SecretsManager::new(environment.clone(), PathBuf::from("./secrets"))?;
            manager.initialize_vault(&master_password).await?;
            info!("✅ Secrets vault initialized successfully");
        }
        Commands::Add { environment, key, value } => {
            info!("Adding secret '{}' to environment: {}", key, environment);
            println!("Enter master password:");
            let master_password = read_password()?;

            let mut manager = get_secrets_manager(&environment).await?;
            manager.set_api_key(&key, &value, &master_password).await?;
            info!("✅ Secret '{}' stored successfully", key);
        }
        Commands::Get { environment, key } => {
            info!("Retrieving secret '{}' from environment: {}", key, environment);
            println!("Enter master password:");
            let master_password = read_password()?;

            let manager = get_secrets_manager(&environment).await?;
            match manager.get_api_key(&key).await {
                Ok(Some(value)) => println!("{}", value),
                Ok(None) => println!("Secret '{}' not found", key),
                Err(e) => {
                    error!("Failed to retrieve secret '{}': {}", key, e);
                    return Err(e);
                }
            }
        }
        Commands::List { environment } => {
            info!("Listing secrets in environment: {}", environment);
            println!("Enter master password:");
            let master_password = read_password()?;

            let manager = get_secrets_manager(&environment).await?;
            // Note: This is a simplified implementation
            // The actual implementation would need to list available keys
            println!("Secrets functionality for environment: {}", environment);
            println!("(Note: List functionality would require additional implementation)");
        }
        Commands::ChangePassword { environment } => {
            info!("Changing master password for environment: {}", environment);
            println!("Enter current master password:");
            let old_password = read_password()?;
            println!("Enter new master password:");
            let new_password = read_password()?;

            let mut manager = get_secrets_manager(&environment).await?;
            manager.rotate_master_password(&old_password, &new_password).await?;
            info!("✅ Master password changed successfully");
        }
    }

    Ok(())
}
