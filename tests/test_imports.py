def test_repo_files_exist():
    # smoke test: expected snapshot files exist
    import pathlib
    p = pathlib.Path("src/opt")
    assert p.exists(), "src/opt missing"
