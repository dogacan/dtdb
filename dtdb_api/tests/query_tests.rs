use dtdb_api::sql_query;

#[test]
fn test_sql_query_macro_and_interpolation() {
    // 1. Test basic bindings and escaping
    let query =
        sql_query!("SELECT * FROM users WHERE id = @id AND name = @name AND details = @details")
            .bind("id", 42i64)
            .bind("name", "Alice's Laptop")
            .bind("details", "Some 'escaped' \"value\" @not_a_param");

    let interpolated = query.interpolate().unwrap();
    assert_eq!(
        interpolated,
        "SELECT * FROM users WHERE id = 42 AND name = 'Alice''s Laptop' AND details = 'Some ''escaped'' \"value\" @not_a_param'"
    );

    // 2. Test raw bytes binding
    let query =
        sql_query!("SELECT * FROM items WHERE hash = @hash").bind("hash", vec![1u8, 2u8, 255u8]);
    assert_eq!(
        query.interpolate().unwrap(),
        "SELECT * FROM items WHERE hash = x'0102ff'"
    );

    // 3. Verify that quotes protect placeholders from replacement
    let query = sql_query!("SELECT * FROM users WHERE email = 'support@id.com' AND id = @id")
        .bind("id", 10i64);
    assert_eq!(
        query.interpolate().unwrap(),
        "SELECT * FROM users WHERE email = 'support@id.com' AND id = 10"
    );

    // 4. Verify unbound parameter results in an error
    let query = sql_query!("SELECT * FROM users WHERE id = @id AND age = @age").bind("id", 10i64);
    assert!(query.interpolate().is_err());
}
