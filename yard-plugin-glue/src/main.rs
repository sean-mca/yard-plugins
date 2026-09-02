mod config;
mod handler;

use handler::GlueHandler;
use yard_plugin_sdk::PluginServer;

fn main() -> ! {
    PluginServer::run(GlueHandler::new())
}
