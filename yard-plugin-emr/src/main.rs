mod config;
mod handler;

use handler::EmrHandler;
use yard_plugin_sdk::PluginServer;

fn main() -> ! {
    PluginServer::run(EmrHandler::new())
}
