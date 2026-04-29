from src.splunksearch import SplunkSearch, configure_splunk_search


def test_splunk_search_posts_query_and_parses_results(monkeypatch) -> None:
    configure_splunk_search(
        base_url="https://splunk.example.com:8089",
        credential_key="cred-key",
        splunk_token="token-value",
        timeout_sec=10,
    )

    captured: dict[str, object] = {}

    class FakeResponse:
        def __iter__(self):
            return iter(
                [
                    b'{"preview": false, "result": {"host": "h1", "message": "m1"}}\n',
                    b'{"preview": false, "result": {"host": "h2", "message": "m2"}}\n',
                ]
            )

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(req, timeout=0):
        captured["url"] = req.full_url
        captured["timeout"] = timeout
        captured["authorization"] = req.get_header("Authorization")
        captured["credential_key"] = req.get_header("X-credential-key")
        captured["body"] = req.data.decode("utf-8")
        return FakeResponse()

    monkeypatch.setattr("src.splunksearch.urlrequest.urlopen", fake_urlopen)

    rows = SplunkSearch("index=main level=ERROR | table host, message")

    assert captured["url"] == "https://splunk.example.com:8089/services/search/jobs/export"
    assert captured["timeout"] == 10.0
    assert captured["authorization"] == "Splunk token-value"
    assert captured["credential_key"] == "cred-key"
    assert "search=index%3Dmain+level%3DERROR+%7C+table+host%2C+message" in str(captured["body"])
    assert rows == [
        {"host": "h1", "message": "m1"},
        {"host": "h2", "message": "m2"},
    ]