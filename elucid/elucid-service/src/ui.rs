use axum::body::{Body, Bytes};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE};
use axum::http::{HeaderName, HeaderValue, Method, StatusCode};
use axum::response::Response;

const INDEX_PATH: &str = "index.html";
const CONTENT_SECURITY_POLICY: HeaderName = HeaderName::from_static("content-security-policy");
const REFERRER_POLICY: HeaderName = HeaderName::from_static("referrer-policy");
const X_CONTENT_TYPE_OPTIONS: HeaderName = HeaderName::from_static("x-content-type-options");

#[derive(Debug)]
struct EmbeddedAsset {
    path: &'static str,
    bytes: &'static [u8],
}

static ASSETS: &[EmbeddedAsset] = include!(concat!(env!("OUT_DIR"), "/embedded_ui_assets.rs"));

pub(crate) fn response(method: &Method, request_path: &str) -> Option<Response> {
    if method != Method::GET && method != Method::HEAD {
        return None;
    }
    let asset_path = request_path.strip_prefix('/').unwrap_or(request_path);
    if let Some(asset) = find_asset(asset_path) {
        return Some(asset_response(method, asset));
    }
    if is_spa_route(request_path) {
        return find_asset(INDEX_PATH).map(|asset| asset_response(method, asset));
    }
    None
}

fn find_asset(path: &str) -> Option<&'static EmbeddedAsset> {
    ASSETS
        .binary_search_by_key(&path, |asset| asset.path)
        .ok()
        .map(|index| &ASSETS[index])
}

fn is_spa_route(path: &str) -> bool {
    !is_reserved_path(path)
        && !path.starts_with("/assets/")
        && !path
            .rsplit('/')
            .next()
            .is_some_and(|segment| segment.contains('.'))
}

fn is_reserved_path(path: &str) -> bool {
    ["/api", "/health", "/metrics"].iter().any(|prefix| {
        path == *prefix
            || path
                .strip_prefix(prefix)
                .is_some_and(|rest| rest.starts_with('/'))
    })
}

fn asset_response(method: &Method, asset: &EmbeddedAsset) -> Response {
    let body = if method == Method::HEAD {
        Body::empty()
    } else {
        Body::from(Bytes::from_static(asset.bytes))
    };
    let mut response = Response::new(body);
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static(content_type(asset.path)),
    );
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static(if asset.path == INDEX_PATH {
            "no-cache"
        } else if asset.path.starts_with("assets/") {
            "public, max-age=31536000, immutable"
        } else {
            "no-cache"
        }),
    );
    headers.insert(
        CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; base-uri 'none'; connect-src 'self'; font-src 'self'; frame-ancestors 'none'; img-src 'self' data:; object-src 'none'; script-src 'self'; style-src 'self' 'unsafe-inline'",
        ),
    );
    headers.insert(REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    response
}

fn content_type(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("css") => "text/css; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("ico") => "image/x-icon",
        Some("js") => "text/javascript; charset=utf-8",
        Some("json") => "application/json; charset=utf-8",
        Some("png") => "image/png",
        Some("svg") => "image/svg+xml",
        Some("webp") => "image/webp",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some(_) | None => "application/octet-stream",
    }
}
