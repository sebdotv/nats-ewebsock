use serde::Deserialize;

/// Information about the connected NATS server.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerInfo {
    pub server_id: String,
    pub server_name: String,
    pub version: String,
    pub go: String,
    pub host: String,
    pub port: u16,
    pub headers: bool,
    pub max_payload: usize,
    pub proto: u8,

    pub client_id: Option<u64>,
    pub git_commit: Option<String>,
    pub client_ip: Option<String>,

    /// Undocumented, returned by version 2.12.1
    pub api_lvl: Option<u8>,
    /// Undocumented, returned by version 2.12.1
    pub xkey: Option<String>,
}
