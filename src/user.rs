use color_eyre::{
    Result,
    eyre::{self, Context},
};
use std::collections::HashMap;

#[derive(Debug, Clone, Default)]
pub struct User {
    pub name: String,
    pub mes: usize,
    pub not_mes: usize,
    pub sparks: usize,
    pub spark_cheats: usize,
    pub meable_message_count: usize,
    pub own_mes_count: usize,
}

impl User {
    pub fn new(name: String) -> Self {
        User {
            name,
            ..Default::default()
        }
    }

    #[must_use]
    pub fn total(&self) -> usize {
        self.mes + self.not_mes
    }

    #[must_use]
    pub fn own_mes_percent(&self) -> String {
        if self.meable_message_count > 0 {
            format!(
                "{:.1}",
                (self.own_mes_count as f64 / self.meable_message_count as f64) * 100.0
            )
        } else {
            "-".to_string()
        }
    }
}

pub struct Users {
    pub id_mapping: HashMap<String, String>,
    pub by_name: HashMap<String, User>,
}

pub fn get_users() -> Result<Users> {
    let current_dir = std::env::current_dir()?;
    let users_path = current_dir.join("users.json");
    let user_map: HashMap<String, String> =
        serde_json::from_reader(std::fs::File::open(&users_path)?).wrap_err_with(|| {
            eyre::eyre!("failed to parse users.json from {}", users_path.display())
        })?;

    let users = user_map
        .values()
        .map(|name| (name.clone(), User::new(name.clone())))
        .collect();

    Ok(Users {
        id_mapping: user_map,
        by_name: users,
    })
}
