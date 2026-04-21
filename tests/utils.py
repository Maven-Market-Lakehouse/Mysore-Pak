def assert_not_empty(df):
    assert df.count() > 0


def assert_no_nulls(df, cols):
    for c in cols:
        assert df.filter(f"{c} IS NULL").count() == 0


def assert_positive(df, col):
    assert df.filter(f"{col} <= 0").count() == 0


def assert_unique(df, col):
    total = df.count()
    distinct = df.select(col).distinct().count()
    assert total == distinct


def assert_fk(df, dim_df, key):
    invalid = df.join(dim_df, key, "left_anti")
    assert invalid.count() == 0


def assert_reconciliation(lower_df, upper_df):
    assert upper_df.count() <= lower_df.count()