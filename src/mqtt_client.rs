use std::{env, error::Error, thread, time::Duration};

use paho_mqtt::{
    AsyncClient, ConnectOptionsBuilder, CreateOptionsBuilder, Message, MQTT_VERSION_3_1_1,
};
use tracing::{debug, error, info};

lazy_static::lazy_static! {
    pub static ref  MQTT_ENABLED : bool= env::var("MQTT_ENABLED").ok()
        .and_then(|v: String| v.parse::<bool>().ok())
        .unwrap_or(false);
    pub static ref TOPIC_PUBLISHING: String = {
        env::var("MQTT_TOPIC_PUBLISHING").unwrap_or_else(|_| "sms".into())
    };
    static ref SERVER_URI: String =  {
        let host = env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".into());
        let port = env::var("MQTT_PORT").unwrap_or_else(|_| "1883".into());

        format!("tcp://{host}:{port}")
    };
    static ref CLIENT_ID: String = env::var("MQTT_CLIENT_ID").unwrap_or_else(|_| "s3_mqtt_subscriber".into());
    static ref USERNAME: String = env::var("MQTT_USERNAME").unwrap_or_else(|_| "root".into());
    static ref PASSWORD: String = env::var("MQTT_PASSWORD").unwrap_or_else(|_| "root".into());

}

pub struct Client {
    cli: AsyncClient,
    options: ClientOptions,
}
#[derive(Debug)]
pub struct ClientOptions {
    server_uri: String,
    client_id: String,
    username: String,
    password: String,
    wait_before_reconnect: u64,
}

impl Default for ClientOptions {
    fn default() -> Self {
        Self {
            server_uri: SERVER_URI.clone(),
            client_id: CLIENT_ID.clone(),
            username: USERNAME.clone(),
            password: PASSWORD.clone(),
            wait_before_reconnect: 2500,
        }
    }
}

impl Client {
    pub fn new(options: ClientOptions) -> Result<Client, Box<dyn Error>> {
        debug!("create client with options {options:?}");
        let create_opts = CreateOptionsBuilder::new()
            .server_uri(options.server_uri.clone())
            .mqtt_version(MQTT_VERSION_3_1_1)
            .client_id(options.client_id.clone())
            .finalize();
        let cli = AsyncClient::new(create_opts)?;

        Ok(Client { cli, options })
    }

    pub async fn connect<FCB>(
        &self,
        connection_callback: FCB,
    ) -> Result<AsyncClient, Box<dyn Error>>
    where
        FCB: Fn(&AsyncClient, u16) + 'static + Copy + Send,
    {
        self.cli.set_connected_callback(|_| info!("Connected"));
        let waiting_millis = self.options.wait_before_reconnect;

        let sleep_before_reconnect = move |rc: i32| {
            error!("Connection attempt failed with error code {}.\n", rc);
            thread::sleep(Duration::from_millis(waiting_millis));
        };

        let on_failure = move |cli: &AsyncClient, _msgid: u16, rc: i32| {
            sleep_before_reconnect(rc);
            cli.reconnect();
        };

        let on_connection_failure = move |cli: &AsyncClient, _msgid: u16, rc: i32| {
            sleep_before_reconnect(rc);
            cli.reconnect_with_callbacks(connection_callback, on_failure);
        };
        self.cli
            .set_disconnected_callback(move |cli: &AsyncClient, _, _| {
                sleep_before_reconnect(1000);
                cli.reconnect_with_callbacks(connection_callback, on_failure);
            });
        self.cli
            .set_connection_lost_callback(move |cli: &AsyncClient| {
                cli.reconnect_with_callbacks(connection_callback, on_connection_failure);
            });

        let username = &self.options.username;
        let password = &self.options.password;
        debug!("username {}, password {}", username, password);
        let lwt = Message::new("test", "Async subscriber lost connection", 1);

        let conn_opts = ConnectOptionsBuilder::new()
            .keep_alive_interval(Duration::from_secs(20))
            .clean_session(true)
            .user_name(username.clone())
            .password(password.clone())
            .will_message(lwt)
            .finalize();
        info!("Connecting to the MQTT server...");
        let _token = self.cli.connect(conn_opts).await?;
        //self.cli.connect_with_callbacks(conn_opts, connection_callback, on_connection_failure);
        Ok(self.cli.clone()) // we don't need to subscribe to any topic for now, so we just return
                             // the inner cli
    }

    pub fn _on_message<F>(&self, cb: F)
    where
        F: FnMut(&AsyncClient, Option<Message>) + 'static + Send,
    {
        self.cli.set_message_callback(cb);
    }
}
