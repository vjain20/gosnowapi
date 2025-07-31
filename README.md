# GoSnowAPI

A lightweight Go client for the [Snowflake SQL API](https://docs.snowflake.com/en/developer-guide/sql-api/index), supporting both synchronous and asynchronous query execution with polling and cancellation.

## Features

- ✅ Execute SQL queries via Snowflake SQL API  
- 🔁 Async execution with polling via `WaitUntilComplete`  
- 🛑 Cancel long-running statements  
- 🔐 JWT-based authentication using RSA key pair (no password/token needed)  
- 🧪 Unit-tested retry and error-handling logic  
- 💡 Minimal and idiomatic Go design  

---

## Installation

```bash
go get github.com/vjain20/gosnowapi
```

---

## Setup

### Requirements

- Snowflake account
- RSA public/private key pair (in PEM format)
- Appropriate role/warehouse/database/schema access

---

## Example

### Basic Client Initialization

```go
privKey, _ := os.ReadFile("testdata/rsa_key.p8")
pubKey, _ := os.ReadFile("testdata/rsa_key.pub")

client, err := snowapi.NewClient(snowapi.Config{
    Account:     "your-account-id",
    User:        "your-username",
    Role:        "SYSADMIN",
    Database:    "TEST_DB",
    Schema:      "PUBLIC",
    Warehouse:   "COMPUTE_WH",
    PrivateKey:  privKey,
    PublicKey:   pubKey,
    ExpireAfter: time.Minute,
})
if err != nil {
    log.Fatal(err)
}
```

### 🌐 PrivateLink & Custom Host Configuration

By default, `gosnowapi` connects to:

```
https://<account>.snowflakecomputing.com/api/v2/statements
```

If you use **AWS PrivateLink** or a **custom domain** for Snowflake, you can configure the base URL like so:

---

#### ✅ Option 1: Use PrivateLink

Set the `PrivateLink` flag in your config:

```go
cfg := snowapi.Config{
    Account:     "your_account",
    User:        "your_user",
    PrivateKey:  privateKeyBytes,
    PublicKey:   publicKeyBytes,
    PrivateLink: true,
}
```

This results in:

```
https://<account>.privatelink.snowflakecomputing.com/api/v2/statements
```

---

#### ✅ Option 2: Override the Full Host Domain

Use `OverrideHost` for full control:

```go
cfg := snowapi.Config{
    Account:      "your_account",
    User:         "your_user",
    PrivateKey:   privateKeyBytes,
    PublicKey:    publicKeyBytes,
    OverrideHost: "custom.snowflakeproxy.internal",
}
```

This results in:

```
https://<account>.custom.snowflakeproxy.internal/api/v2/statements
```

---

> **Note:** If both `PrivateLink` and `OverrideHost` are set, `OverrideHost` takes precedence.

---

## Executing Queries

### Synchronous Query

```go
rows, err := client.Query("SELECT current_version()")
if err != nil {
    log.Fatal(err)
}
fmt.Println("Result:", rows)
```

---

### Asynchronous Query

```go
resp, err := client.Execute("SELECT SYSTEM$WAIT(5)", true, &snowapi.RequestOptions{})
if err != nil {
    log.Fatal(err)
}

fmt.Println("Submitted async query, handle:", resp.StatementHandle)

finalResp, err := client.WaitUntilComplete(resp.StatementHandle, 2*time.Second, 10)
if err != nil {
    log.Fatal(err)
}

fmt.Println("Final result:", finalResp.Data)
```

---

### Canceling a Query

```go
err := client.Cancel("your-statement-handle")
if err != nil {
    log.Fatal("Cancel failed:", err)
}
fmt.Println("Query cancelled successfully.")
```

---

## Testing

Run all tests:

```bash
go test ./...
```

Includes:

- Retry logic  
- Polling behavior  
- Timeout handling  
- Error codes from Snowflake

---

## License

MIT

---

## Author

[Vaibhav Jain](https://github.com/vjain20)
