use dtdb_relational::{Column, DataType, Database, IndexType, RelationalError, Schema};
use tempfile::TempDir;

fn create_test_schema() -> Schema {
    Schema::new(vec![
        Column {
            name: "id".to_string(),
            data_type: DataType::Int,
            is_primary_key: true,
            is_nullable: false,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
        Column {
            name: "name".to_string(),
            data_type: DataType::String,
            is_primary_key: false,
            is_nullable: true,
            locality_group: None,
            default_value: None,
            is_auto_increment: false,
        },
    ])
}

#[test]
fn test_schema_version_lifecycle() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();

    // 1. Initial version is 0
    assert_eq!(db.schema_version(), 0);

    // 2. Successful create_table increments version to 1
    db.create_table("users", create_test_schema()).unwrap();
    assert_eq!(db.schema_version(), 1);

    // 3. Successful create_index increments version to 2
    db.create_index(
        "users",
        "idx_name",
        vec!["name".to_string()],
        IndexType::BTree,
        None,
    )
    .unwrap();
    assert_eq!(db.schema_version(), 2);

    // 4. Successful drop_index increments version to 3
    db.drop_index("users", "idx_name").unwrap();
    assert_eq!(db.schema_version(), 3);

    // 5. Successful drop_table increments version to 4
    db.drop_table("users").unwrap();
    assert_eq!(db.schema_version(), 4);
}

#[test]
fn test_schema_version_failed_ddl_does_not_increment() {
    let temp_dir = TempDir::new().unwrap();
    let db = Database::open(temp_dir.path()).unwrap();

    assert_eq!(db.schema_version(), 0);

    // Create table successfully -> version 1
    db.create_table("users", create_test_schema()).unwrap();
    assert_eq!(db.schema_version(), 1);

    // Try to create already existing table -> should fail and NOT increment version
    let res = db.create_table("users", create_test_schema());
    assert!(matches!(res, Err(RelationalError::TableAlreadyExists(_))));
    assert_eq!(db.schema_version(), 1);

    // Try to drop non-existing table -> should fail and NOT increment version
    let res = db.drop_table("non_existent");
    assert!(matches!(res, Err(RelationalError::TableNotFound(_))));
    assert_eq!(db.schema_version(), 1);

    // Try to create index on non-existing table -> should fail and NOT increment
    let res = db.create_index(
        "non_existent",
        "idx_name",
        vec!["name".to_string()],
        IndexType::BTree,
        None,
    );
    assert!(matches!(res, Err(RelationalError::TableNotFound(_))));
    assert_eq!(db.schema_version(), 1);

    // Try to drop index on non-existing table -> should fail and NOT increment
    let res = db.drop_index("non_existent", "idx_name");
    assert!(matches!(res, Err(RelationalError::TableNotFound(_))));
    assert_eq!(db.schema_version(), 1);

    // Try to create index on columns that don't exist -> should fail and NOT increment
    let res = db.create_index(
        "users",
        "idx_bad",
        vec!["non_existent_column".to_string()],
        IndexType::BTree,
        None,
    );
    assert!(matches!(res, Err(RelationalError::SchemaMismatch(_))));
    assert_eq!(db.schema_version(), 1);

    // Try to drop non-existing index on existing table -> should fail and NOT increment
    let res = db.drop_index("users", "non_existent_idx");
    assert!(matches!(res, Err(RelationalError::SchemaMismatch(_))));
    assert_eq!(db.schema_version(), 1);
}
