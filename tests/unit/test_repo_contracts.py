from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]


def test_compose_services_are_health_checked_and_loopback_bound() -> None:
    compose = yaml.safe_load((ROOT / "docker-compose.yml").read_text())
    assert set(compose["services"]) == {"spark", "minio", "minio-init", "postgres"}
    assert all("healthcheck" in service for service in compose["services"].values())
    for service in compose["services"].values():
        assert all(str(port).startswith("127.0.0.1:") for port in service.get("ports", []))


def test_config_profiles_have_identical_keys() -> None:
    local = yaml.safe_load((ROOT / "conf/local.yml").read_text())
    remote = yaml.safe_load((ROOT / "conf/databricks.yml").read_text())
    assert set(local) == set(remote)
    assert local["min_vacuum_retain_hours"] >= 168


def test_required_make_targets_exist() -> None:
    makefile = (ROOT / "Makefile").read_text()
    targets = {line.split(":", 1)[0] for line in makefile.splitlines() if ": ## " in line}
    assert targets == {
        "help",
        "lint",
        "format",
        "test-unit",
        "test-integration",
        "test",
        "bootstrap",
        "teardown",
        "seed",
        "seed-scale10",
        "run-bronze",
        "run-silver",
        "run-gold",
        "run-all",
        "verify",
        "sql",
        "maintenance",
        "test-maintenance-scale10",
    }


def test_cfop_reference_is_a_versioned_package_resource() -> None:
    resource = ROOT / "src/lakehouse_fiscal/gold/data/cfop_reference.csv"
    assert resource.is_file()
    assert resource.read_text(encoding="utf-8").startswith(
        "cfop,grupo,tipo_operacao,gera_receita,descricao"
    )
    assert "/data/" in (ROOT / ".gitignore").read_text(encoding="utf-8")


def test_teardown_removes_persistent_local_catalog() -> None:
    teardown = (ROOT / "scripts/teardown_local.sh").read_text(encoding="utf-8")
    assert "metastore_db spark-warehouse" in teardown
    assert "derby.log" in teardown
