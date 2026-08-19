use std::time::Duration;

use elucid_catalog::{InputName, SourceName};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, RequestBuilder, Response, StatusCode};

use crate::arguments::{ClientTimeoutSeconds, ProductEndpoint};
use crate::error::{CliErrorCode, Failure};
use crate::input::RequestInput;

const MAXIMUM_CLIENT_RESPONSE_BYTES: usize = 1_048_576;

#[derive(Debug)]
pub(crate) struct ProductClient {
    client: Client,
}

impl ProductClient {
    pub(crate) fn new(timeout_seconds: ClientTimeoutSeconds) -> Result<Self, Failure> {
        let client = Client::builder()
            .timeout(Duration::from_secs(timeout_seconds.get()))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| {
                Failure::internal(
                    CliErrorCode::HttpClientInitializationFailed,
                    anyhow::Error::new(error).context("failed to initialize HTTP client"),
                )
            })?;
        Ok(Self { client })
    }

    pub(crate) async fn apply_catalog(
        &self,
        endpoint: &ProductEndpoint,
        input: RequestInput,
    ) -> Result<HttpResponse, Failure> {
        let body = input.into_body().await?;
        let request = self
            .client
            .post(endpoint.catalog_application_url())
            .header(CONTENT_TYPE, "application/yaml")
            .body(body);
        send(request).await
    }

    pub(crate) async fn submit_ingestion(
        &self,
        endpoint: &ProductEndpoint,
        source: &SourceName,
        input_name: &InputName,
        input: RequestInput,
    ) -> Result<HttpResponse, Failure> {
        let url = endpoint
            .ingestion_url(source, input_name)
            .map_err(|error| {
                Failure::internal(
                    CliErrorCode::EndpointUrlConstructionFailed,
                    anyhow::anyhow!(error),
                )
            })?;
        let body = input.into_body().await?;
        let request = self
            .client
            .post(url)
            .header(CONTENT_TYPE, "application/x-ndjson")
            .body(body);
        send(request).await
    }
}

#[derive(Debug)]
pub(crate) struct HttpResponse {
    status: StatusCode,
    body: Vec<u8>,
}

impl HttpResponse {
    pub(crate) const fn status(&self) -> StatusCode {
        self.status
    }

    pub(crate) fn into_body(self) -> Vec<u8> {
        self.body
    }
}

async fn send(request: RequestBuilder) -> Result<HttpResponse, Failure> {
    let response = request.send().await.map_err(Failure::from_request_error)?;
    read_bounded_response(response).await
}

async fn read_bounded_response(mut response: Response) -> Result<HttpResponse, Failure> {
    validate_response_content_type(&response)?;
    if response
        .content_length()
        .is_some_and(|length| length > MAXIMUM_CLIENT_RESPONSE_BYTES as u64)
    {
        return Err(Failure::remote_unavailable(
            CliErrorCode::RemoteResponseTooLarge,
            anyhow::anyhow!("remote response exceeds {MAXIMUM_CLIENT_RESPONSE_BYTES} bytes"),
        ));
    }
    let status = response.status();
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(Failure::from_request_error)?
    {
        let next_length = body.len().checked_add(chunk.len()).ok_or_else(|| {
            Failure::remote_unavailable(
                CliErrorCode::RemoteResponseTooLarge,
                anyhow::anyhow!("remote response size overflowed"),
            )
        })?;
        if next_length > MAXIMUM_CLIENT_RESPONSE_BYTES {
            return Err(Failure::remote_unavailable(
                CliErrorCode::RemoteResponseTooLarge,
                anyhow::anyhow!("remote response exceeds {MAXIMUM_CLIENT_RESPONSE_BYTES} bytes"),
            ));
        }
        body.extend_from_slice(&chunk);
    }
    validate_response_body(&body)?;
    Ok(HttpResponse { status, body })
}

fn validate_response_content_type(response: &Response) -> Result<(), Failure> {
    let is_json = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|media_type| media_type.trim().eq_ignore_ascii_case("application/json"));
    if !is_json {
        return Err(Failure::remote_unavailable(
            CliErrorCode::RemoteResponseInvalid,
            anyhow::anyhow!("remote response Content-Type is not application/json"),
        ));
    }
    Ok(())
}

fn validate_response_body(body: &[u8]) -> Result<(), Failure> {
    match serde_json::from_slice::<serde_json::Value>(body) {
        Ok(serde_json::Value::Object(_)) => Ok(()),
        Ok(_) => Err(Failure::remote_unavailable(
            CliErrorCode::RemoteResponseInvalid,
            anyhow::anyhow!("remote response body is not a JSON object"),
        )),
        Err(error) => Err(Failure::remote_unavailable(
            CliErrorCode::RemoteResponseInvalid,
            anyhow::Error::new(error).context("remote response body is not valid JSON"),
        )),
    }
}
