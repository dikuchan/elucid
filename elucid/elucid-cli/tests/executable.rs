use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const ELUCID: &str = env!("CARGO_BIN_EXE_elucid");
const MAXIMUM_TEST_REQUEST_BYTES: usize = 2 * 1024 * 1024;

#[test]
fn executable_exposes_the_supported_command_tree() {
    let help = Command::new(ELUCID)
        .arg("--help")
        .output()
        .expect("run elucid --help");

    assert!(help.status.success());
    let stdout = String::from_utf8(help.stdout).expect("help is UTF-8");
    for command in ["server", "catalog", "ingestion"] {
        assert!(
            stdout
                .lines()
                .any(|line| line.trim_start().starts_with(command)),
            "missing {command:?} in help:\n{stdout}"
        );
    }

    for obsolete_command in ["execute", "repl", "schema", "validate"] {
        let output = Command::new(ELUCID)
            .arg(obsolete_command)
            .output()
            .expect("run obsolete command");
        assert_eq!(output.status.code(), Some(2), "{obsolete_command}");
    }
}

#[test]
fn json_version_exposes_the_packaging_contract() {
    let output = Command::new(ELUCID)
        .args(["--version", "--output", "json"])
        .output()
        .expect("run elucid --version");

    assert!(output.status.success());
    let value: serde_json::Value = serde_json::from_slice(&output.stdout).expect("version JSON");
    let object = value.as_object().expect("version is an object");
    assert_eq!(object.len(), 6);
    assert_eq!(object["semantic_version"], env!("CARGO_PKG_VERSION"));
    assert!(object["git_revision"].is_null() || object["git_revision"].is_string());
    assert!(object["build_profile"].is_string());
    assert!(
        object["frontend_asset_revision"].is_null()
            || object["frontend_asset_revision"].is_string()
    );
    assert_eq!(object["storage_format_version"], 1);
    assert_eq!(
        object["supported_metastore_migration_range"],
        serde_json::json!({"minimum_version": 1, "maximum_version": 1})
    );
}

#[test]
fn invalid_server_configuration_exits_with_configuration_failure() {
    let mut configuration = tempfile::NamedTempFile::new().expect("temporary configuration");
    configuration
        .write_all(b"server = [")
        .expect("write invalid configuration");

    let output = Command::new(ELUCID)
        .args(["server", "--config"])
        .arg(configuration.path())
        .output()
        .expect("run elucid server");

    assert_eq!(output.status.code(), Some(3));
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("CONFIGURATION_DOCUMENT_MALFORMED"));
}

#[test]
fn server_is_the_foreground_entrypoint_without_a_run_subcommand() {
    let output = Command::new(ELUCID)
        .args(["server", "run", "--config", "unused.toml"])
        .output()
        .expect("run obsolete server subcommand");

    assert_eq!(output.status.code(), Some(2));
}

#[test]
fn catalog_apply_sends_exact_file_bytes_without_authentication() {
    let response = br#"{"outcome":"UNCHANGED"}"#;
    let server = TestServer::start("200 OK", response, ResponseTiming::Immediate);
    let manifest = b"format_version: 1\r\nsource:\r\n  name: demo_logs";
    let mut file = tempfile::NamedTempFile::new().expect("temporary manifest");
    file.write_all(manifest).expect("write manifest");
    let output = Command::new(ELUCID)
        .args(["catalog", "apply", "--endpoint"])
        .arg(server.endpoint())
        .args(["--file"])
        .arg(file.path())
        .output()
        .expect("run catalog apply");

    assert!(output.status.success());
    assert_eq!(output.stdout, response);
    let request = server.finish();
    assert_eq!(request.method, "POST");
    assert_eq!(request.target, "/api/v1/catalog-applications");
    assert_eq!(request.header("content-type"), Some("application/yaml"));
    assert_eq!(request.header("authorization"), None);
    assert_eq!(request.header("idempotency-key"), None);
    assert_eq!(request.body, manifest);
}

#[test]
fn ingestion_submit_sends_exact_standard_input_without_authentication_or_idempotency() {
    let response = br#"{"state":"DURABLY_QUEUED"}"#;
    let server = TestServer::start("202 Accepted", response, ResponseTiming::Immediate);
    let body = b"{\"status\":200}\r\n\r\n{\"status\":503}";
    let mut child = Command::new(ELUCID)
        .args([
            "ingestion",
            "submit",
            "--endpoint",
            server.endpoint(),
            "--source",
            "demo_logs",
            "--input",
            "http",
            "--file",
            "-",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run ingestion submit");
    child
        .stdin
        .take()
        .expect("child stdin")
        .write_all(body)
        .expect("write child stdin");
    let output = child.wait_with_output().expect("wait for ingestion submit");

    assert!(output.status.success());
    assert_eq!(output.stdout, response);
    let request = server.finish();
    assert_eq!(request.method, "POST");
    assert_eq!(
        request.target,
        "/api/v1/sources/demo_logs/inputs/http/events"
    );
    assert_eq!(request.header("content-type"), Some("application/x-ndjson"));
    assert_eq!(request.header("authorization"), None);
    assert_eq!(request.header("idempotency-key"), None);
    assert_eq!(request.body, body);
}

#[test]
fn retained_http_helpers_reject_retired_authentication_and_idempotency_flags() {
    let catalog = Command::new(ELUCID)
        .args([
            "catalog",
            "apply",
            "--endpoint",
            "http://127.0.0.1:1",
            "--file",
            "-",
            "--operator-bearer-token-environment-variable",
            "TOKEN",
        ])
        .output()
        .expect("run catalog helper with retired authentication flag");
    assert_eq!(catalog.status.code(), Some(2));

    let ingestion = Command::new(ELUCID)
        .args([
            "ingestion",
            "submit",
            "--endpoint",
            "http://127.0.0.1:1",
            "--source",
            "demo_logs",
            "--input",
            "http",
            "--file",
            "-",
            "--idempotency-key",
            "retired",
        ])
        .output()
        .expect("run ingestion helper with retired idempotency flag");
    assert_eq!(ingestion.status.code(), Some(2));
}

#[test]
fn remote_responses_have_stable_exit_categories() {
    assert_remote_exit(
        RemoteOperation::Catalog,
        "409 Conflict",
        br#"{"error":{"code":"CATALOG_DEFINITION_CONFLICT"}}"#,
        5,
    );
    assert_remote_exit(
        RemoteOperation::Ingestion,
        "413 Content Too Large",
        br#"{"error":{"code":"INGESTION_BATCH_LIMIT_EXCEEDED"}}"#,
        2,
    );
    assert_remote_exit(
        RemoteOperation::Ingestion,
        "429 Too Many Requests",
        br#"{"error":{"code":"CAPACITY_EXHAUSTED"}}"#,
        4,
    );
    assert_remote_exit(
        RemoteOperation::Catalog,
        "503 Service Unavailable",
        br#"{"error":{"code":"METASTORE_UNAVAILABLE"}}"#,
        4,
    );
}

#[test]
fn local_client_timeout_exits_with_timeout_failure() {
    let server = TestServer::start(
        "201 Created",
        br#"{"outcome":"CREATED"}"#,
        ResponseTiming::After(Duration::from_millis(1_500)),
    );

    let output = Command::new(ELUCID)
        .args([
            "catalog",
            "apply",
            "--endpoint",
            server.endpoint(),
            "--file",
            "-",
            "--timeout-seconds",
            "1",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("run timed catalog apply");

    assert_eq!(output.status.code(), Some(7));
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(
        stderr.contains("CLIENT_TIMEOUT"),
        "unexpected timeout diagnostic: {stderr:?}"
    );
    let request = server.finish();
    assert!(request.body.is_empty());
}

#[test]
fn client_rejects_non_json_response_without_echoing_remote_bytes() {
    let remote_bytes = b"\x1b[31mnot-json";
    let server = TestServer::start("200 OK", remote_bytes, ResponseTiming::Immediate);

    let output = Command::new(ELUCID)
        .args([
            "catalog",
            "apply",
            "--endpoint",
            server.endpoint(),
            "--file",
            "-",
        ])
        .stdin(Stdio::null())
        .output()
        .expect("run catalog apply against invalid response");

    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
    assert!(
        !output
            .stderr
            .windows(remote_bytes.len())
            .any(|bytes| bytes == remote_bytes)
    );
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    assert!(stderr.contains("REMOTE_RESPONSE_INVALID"));
    let _request = server.finish();
}

fn assert_remote_exit(
    operation: RemoteOperation,
    response_status: &str,
    response: &[u8],
    expected_exit: i32,
) {
    let server = TestServer::start(response_status, response, ResponseTiming::Immediate);
    let mut command = Command::new(ELUCID);
    match operation {
        RemoteOperation::Catalog => {
            command.args([
                "catalog",
                "apply",
                "--endpoint",
                server.endpoint(),
                "--file",
                "-",
            ]);
        }
        RemoteOperation::Ingestion => {
            command.args([
                "ingestion",
                "submit",
                "--endpoint",
                server.endpoint(),
                "--source",
                "demo_logs",
                "--input",
                "http",
                "--file",
                "-",
            ]);
        }
    }
    let output = command
        .stdin(Stdio::null())
        .output()
        .expect("run remote command");

    assert_eq!(output.status.code(), Some(expected_exit));
    assert_eq!(output.stderr, response);
    let _request = server.finish();
}

#[derive(Clone, Copy)]
enum RemoteOperation {
    Catalog,
    Ingestion,
}

#[derive(Clone, Copy)]
enum ResponseTiming {
    Immediate,
    After(Duration),
}

struct TestServer {
    endpoint: String,
    requests: Receiver<ReceivedRequest>,
    worker: JoinHandle<Result<(), String>>,
}

impl TestServer {
    fn start(response_status: &str, response_body: &[u8], timing: ResponseTiming) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
        let address = listener.local_addr().expect("test server address");
        let endpoint = format!("http://{address}");
        let response_status = response_status.to_owned();
        let response_body = response_body.to_owned();
        let (request_sender, requests) = mpsc::sync_channel(1);
        let worker = thread::spawn(move || {
            let (mut stream, _) = listener.accept().map_err(|error| error.to_string())?;
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .map_err(|error| error.to_string())?;
            let request = read_request(&mut stream)?;
            request_sender
                .send(request)
                .map_err(|error| error.to_string())?;
            if let ResponseTiming::After(duration) = timing {
                thread::sleep(duration);
            }
            let response_head = format!(
                "HTTP/1.1 {response_status}\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                response_body.len()
            );
            let write_result = stream
                .write_all(response_head.as_bytes())
                .and_then(|()| stream.write_all(&response_body));
            match (timing, write_result) {
                (ResponseTiming::Immediate, result) => result.map_err(|error| error.to_string()),
                (ResponseTiming::After(_), Ok(())) => Ok(()),
                (ResponseTiming::After(_), Err(error))
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::BrokenPipe
                            | std::io::ErrorKind::ConnectionReset
                            | std::io::ErrorKind::ConnectionAborted
                    ) =>
                {
                    Ok(())
                }
                (ResponseTiming::After(_), Err(error)) => Err(error.to_string()),
            }
        });
        Self {
            endpoint,
            requests,
            worker,
        }
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn finish(self) -> ReceivedRequest {
        let request = self
            .requests
            .recv_timeout(Duration::from_secs(5))
            .expect("receive HTTP request");
        self.worker
            .join()
            .expect("join test server")
            .expect("serve HTTP request");
        request
    }
}

struct ReceivedRequest {
    method: String,
    target: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl ReceivedRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

fn read_request(stream: &mut TcpStream) -> Result<ReceivedRequest, String> {
    let mut received = Vec::new();
    let header_end = loop {
        if let Some(index) = find_bytes(&received, b"\r\n\r\n") {
            break index + 4;
        }
        read_more(stream, &mut received)?;
    };
    let head =
        std::str::from_utf8(&received[..header_end - 4]).map_err(|error| error.to_string())?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next().ok_or("missing HTTP request line")?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or("missing HTTP method")?
        .to_owned();
    let target = request_parts
        .next()
        .ok_or("missing HTTP target")?
        .to_owned();
    let mut headers = HashMap::new();
    for line in lines {
        let (name, value) = line.split_once(':').ok_or("malformed HTTP header")?;
        headers.insert(name.to_ascii_lowercase(), value.trim().to_owned());
    }

    let encoded_body = received.split_off(header_end);
    let body = match headers.get("content-length") {
        Some(value) => {
            let length = value.parse::<usize>().map_err(|error| error.to_string())?;
            read_content_length_body(stream, encoded_body, length)?
        }
        None if headers
            .get("transfer-encoding")
            .is_some_and(|value| value.eq_ignore_ascii_case("chunked")) =>
        {
            read_chunked_body(stream, encoded_body)?
        }
        None => encoded_body,
    };
    Ok(ReceivedRequest {
        method,
        target,
        headers,
        body,
    })
}

fn read_content_length_body(
    stream: &mut TcpStream,
    mut bytes: Vec<u8>,
    length: usize,
) -> Result<Vec<u8>, String> {
    if length > MAXIMUM_TEST_REQUEST_BYTES {
        return Err("test request body exceeds bound".to_owned());
    }
    while bytes.len() < length {
        read_more(stream, &mut bytes)?;
    }
    bytes.truncate(length);
    Ok(bytes)
}

fn read_chunked_body(stream: &mut TcpStream, mut encoded: Vec<u8>) -> Result<Vec<u8>, String> {
    let mut decoded = Vec::new();
    let mut position = 0;
    loop {
        let line_end = loop {
            if let Some(relative) = find_bytes(&encoded[position..], b"\r\n") {
                break position + relative;
            }
            read_more(stream, &mut encoded)?;
        };
        let size_token = std::str::from_utf8(&encoded[position..line_end])
            .map_err(|error| error.to_string())?
            .split(';')
            .next()
            .ok_or("missing chunk size")?;
        let size = usize::from_str_radix(size_token, 16).map_err(|error| error.to_string())?;
        position = line_end + 2;
        let chunk_end = position
            .checked_add(size)
            .ok_or("chunk position overflow")?;
        let terminator_end = chunk_end.checked_add(2).ok_or("chunk position overflow")?;
        while encoded.len() < terminator_end {
            read_more(stream, &mut encoded)?;
        }
        if &encoded[chunk_end..terminator_end] != b"\r\n" {
            return Err("invalid chunk terminator".to_owned());
        }
        if size == 0 {
            return Ok(decoded);
        }
        let new_length = decoded
            .len()
            .checked_add(size)
            .ok_or("decoded body size overflow")?;
        if new_length > MAXIMUM_TEST_REQUEST_BYTES {
            return Err("test request body exceeds bound".to_owned());
        }
        decoded.extend_from_slice(&encoded[position..chunk_end]);
        position = terminator_end;
    }
}

fn read_more(stream: &mut TcpStream, bytes: &mut Vec<u8>) -> Result<(), String> {
    if bytes.len() >= MAXIMUM_TEST_REQUEST_BYTES {
        return Err("test request exceeds bound".to_owned());
    }
    let mut buffer = [0_u8; 4096];
    let read = stream
        .read(&mut buffer)
        .map_err(|error| error.to_string())?;
    if read == 0 {
        return Err("HTTP request ended unexpectedly".to_owned());
    }
    bytes.extend_from_slice(&buffer[..read]);
    Ok(())
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
