from sandbox.template_cache import fingerprint, template_db_name


def test_fingerprint_stable_and_order_independent():
    a = fingerprint(
        dialect="mysql",
        db_id="debit_card_specializing",
        tables=["sales", "customers"],
        copy_thr=20000,
        sample_size=20000,
    )
    b = fingerprint(
        dialect="mysql",
        db_id="debit_card_specializing",
        tables=["customers", "sales"],
        copy_thr=20000,
        sample_size=20000,
    )
    assert a == b
    c = fingerprint(
        dialect="mysql",
        db_id="debit_card_specializing",
        tables=["sales"],
        copy_thr=20000,
        sample_size=20000,
    )
    assert a != c


def test_template_names_differ_by_db_and_fit_mysql_limit():
    n1 = template_db_name(
        dialect="mysql", db_id="financial", tables=["account"], copy_thr=1, sample_size=1
    )
    n2 = template_db_name(
        dialect="mysql",
        db_id="debit_card_specializing",
        tables=["account"],
        copy_thr=1,
        sample_size=1,
    )
    n3 = template_db_name(
        dialect="postgresql",
        db_id="financial",
        tables=["account"],
        copy_thr=1,
        sample_size=1,
    )
    assert n1 != n2
    assert n1 != n3
    assert n1.startswith("astratune_tpl_")
    assert len(n1) <= 63
    assert n1 != "astratune_sandbox"
    assert n1 != "astratune_sandbox_0"
