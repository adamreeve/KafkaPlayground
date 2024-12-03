use std::collections::HashMap;

pub fn get_bootstrap_servers(config: &HashMap<String, String>) -> Vec<String> {
    let mut servers = Vec::new();
    if let Some(value) = config.get("bootstrap.servers") {
        for uri in value.split(',') {
            servers.push(uri.to_owned());
        }
    }
    servers
}

pub fn get_topic_name(index: i64) -> String {
    format!("my-topic-{}", index)
}
