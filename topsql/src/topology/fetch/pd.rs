use std::collections::HashSet;

use snafu::{ResultExt, Snafu};

use vector::http::HttpClient;

use crate::topology::{
    fetch::{models, utils},
    Component, InstanceType,
};

#[derive(Debug, Snafu)]
pub enum FetchError {
    #[snafu(display("Failed to build request: {}", source))]
    BuildRequest { source: http::Error },
    #[snafu(display("Failed to get health: {}", source))]
    GetHealth { source: vector::http::HttpError },
    #[snafu(display("Failed to get health text: {}", source))]
    GetHealthBytes { source: hyper::Error },
    #[snafu(display("Failed to parse health JSON text: {}", source))]
    HealthJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to get members: {}", source))]
    GetMembers { source: vector::http::HttpError },
    #[snafu(display("Failed to get members text: {}", source))]
    GetMembersBytes { source: hyper::Error },
    #[snafu(display("Failed to parse members JSON text: {}", source))]
    MembersJsonFromStr { source: serde_json::Error },
    #[snafu(display("Failed to parse pd address: {}", source))]
    ParsePDAddress { source: utils::ParseError },
}

pub struct PDTopologyFetcher<'a> {
    health_path: &'static str,
    members_path: &'static str,

    pd_address: &'a str,
    http_client: &'a HttpClient<hyper::Body>,
}

impl<'a> PDTopologyFetcher<'a> {
    pub fn new(pd_address: &'a str, http_client: &'a HttpClient<hyper::Body>) -> Self {
        Self {
            health_path: "/pd/api/v1/health",
            members_path: "/pd/api/v1/members",

            pd_address,
            http_client,
        }
    }

    pub async fn get_up_pds(&self, components: &mut HashSet<Component>) -> Result<(), FetchError> {
        let health_resp = self.fetch_pd_health().await?;
        let members_resp = self.fetch_pd_members().await?;

        let health_members = health_resp
            .iter()
            .filter_map(|h| h.health.then(|| h.member_id))
            .collect::<HashSet<_>>();
        for member in members_resp.members {
            if health_members.contains(&member.member_id) {
                if let Some(url) = member.client_urls.iter().next() {
                    let (host, port) = utils::parse_host_port(url).context(ParsePDAddressSnafu)?;
                    components.insert(Component {
                        instance_type: InstanceType::PD,
                        host,
                        primary_port: port,
                        secondary_port: port,
                    });
                }
            }
        }

        Ok(())
    }

    async fn fetch_pd_health(&self) -> Result<models::HealthResponse, FetchError> {
        let req = http::Request::get(format!("{}{}", self.pd_address, self.health_path))
            .body(hyper::Body::empty())
            .context(BuildRequestSnafu)?;

        let res = self.http_client.send(req).await.context(GetHealthSnafu)?;

        let body = res.into_body();
        let bytes = hyper::body::to_bytes(body)
            .await
            .context(GetHealthBytesSnafu)?;

        let health_resp = serde_json::from_slice::<models::HealthResponse>(&bytes)
            .context(HealthJsonFromStrSnafu)?;

        Ok(health_resp)
    }

    async fn fetch_pd_members(&self) -> Result<models::MembersResponse, FetchError> {
        let req = http::Request::get(format!("{}{}", self.pd_address, self.members_path))
            .body(hyper::Body::empty())
            .context(BuildRequestSnafu)?;

        let res = self.http_client.send(req).await.context(GetMembersSnafu)?;

        let body = res.into_body();
        let bytes = hyper::body::to_bytes(body)
            .await
            .context(GetMembersBytesSnafu)?;

        let members_resp = serde_json::from_slice::<models::MembersResponse>(&bytes)
            .context(MembersJsonFromStrSnafu)?;

        Ok(members_resp)
    }
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn fetch_health() {}
}
