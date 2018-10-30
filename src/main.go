package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "github.com/gorilla/mux"
    "database/sql"
    _ "github.com/lib/pq"
    "gopkg.in/guregu/null.v3"
)

const (
    DB_HOST = "127.0.0.1"
    DB_PORT = "5433"
    DB_NAME     = "msf"
    DB_USER     = "msf"
    DB_PASSWORD = ""
)

// database connection handle manages pool of connections
// NOTE: designed to be long-lived, "create one sql.DB object
// for each distinct datastore you need to access, and keep it
// until the program is done accessing that datastore."
// http://go-database-sql.org/accessing.html
var DB *sql.DB


func init() {
    fmt.Println("init()...")
    dbinfo := fmt.Sprintf("host=%s port=%s user=%s dbname=%s password=%s sslmode=disable",
        DB_HOST, DB_PORT, DB_USER, DB_NAME, DB_PASSWORD)
    fmt.Println("init(): dbinfo", dbinfo)
    var err error
    DB, err = sql.Open("postgres", dbinfo)
    checkErr(err, true)
    //log.Fatal(“Error: The data source arguments are not valid”)
    // defer db.Close()
    fmt.Println("init(): DB", DB)
    fmt.Println("init(): err", err)

    err = DB.Ping()
    if err != nil {
        log.Fatal("Error: Could not establish a connection with the database")
    }
    checkErr(err, true)

    logDBStats(DB.Stats())
    // See: https://golang.org/src/database/sql/sql.go
    // SetMaxOpenConns sets the maximum number of open connections to the database.
    DB.SetMaxOpenConns(20)

    logDBStats(DB.Stats())

    // SetMaxIdleConns sets the maximum number of connections in the idle
    // connection pool.
    DB.SetMaxIdleConns(20)

    logDBStats(DB.Stats())
}


func HealthCheck(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    response := map[string]string{
        "health": "alive",
    }
    json.NewEncoder(w).Encode(response)
}

func GetDBStats(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    stats := DB.Stats()
    response := map[string]interface{}{
        "MaxOpenConnections": stats.MaxOpenConnections,
        "OpenConnections": stats.OpenConnections,
        "InUse": stats.InUse,
        "Idle": stats.Idle,
        "WaitCount": stats.WaitCount,
        "WaitDuration": stats.WaitDuration,
        "MaxIdleClosed": stats.MaxIdleClosed,
        "MaxLifetimeClosed": stats.MaxLifetimeClosed,
    }
    json.NewEncoder(w).Encode(response)
}

func GetHosts(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    params := r.URL.Query()
    fmt.Println("GetHosts(): vars:", vars)
    fmt.Println("GetHosts(): params:", params)

    rows, err := DB.Query("SELECT id, address, name, state, comments FROM hosts")
    checkErr(err, true)
    defer rows.Close()

    var hosts []map[string]interface{}
    for rows.Next() {
        var id null.Int
        var address null.String
        var name null.String
        var state null.String
        var comments null.String
        err = rows.Scan(&id, &address, &name, &state, &comments)
        checkErr(err, false)

        host := map[string]interface{}{
            "id": id,
            "address": address,
            "name": name,
            "state": state,
            "comments": comments,
        }
        hosts = append(hosts, host)
    }

    err = rows.Err()
    checkErr(err, false)

    response := map[string]interface{}{
        "data": hosts,
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(response)
}

func GetHost(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    fmt.Println("GetHost(): vars:", vars)
    fmt.Println("GetHost(): id:", vars["id"])

    var id null.Int
    var address null.String
    var name null.String
    var state null.String
    var comments null.String

    row := DB.QueryRow("SELECT id, address, name, state, comments FROM hosts WHERE id=$1", vars["id"])
    err := row.Scan(&id, &address, &name, &state, &comments)

    response := make(map[string]interface{})
    switch {
    case err == sql.ErrNoRows:
        log.Printf("No host with ID %v", vars["id"])
        errorMsg := fmt.Sprintf("Couldn't find host with 'id'=%s", vars["id"])
        error := map[string]interface{}{
            "code": 500,
            "message": errorMsg,
        }
        response["error"] = error
    case err != nil:
        log.Fatal(err)
        errorMsg := fmt.Sprintf("There was an error getting host: %s", err)
        error := map[string]interface{}{
            "code": 500,
            "message": errorMsg,
        }
        response["error"] = error
    default:
        log.Printf("Host %v found", id)
        host := map[string]interface{}{
            "id": id,
            "address": address,
            "name": name,
            "state": state,
            "comments": comments,
        }
        response["data"] = host
    }

    // host := map[string]interface{}{
    //     "id": id,
    //     "address": address,
    //     "name": name,
    // }

    // response := map[string]interface{}{
    //     "data": host,
    // }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(response)
}

func CreateHost(w http.ResponseWriter, r *http.Request) {
    var obj map[string]interface{}
    if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
        // handle error
        log.Fatal(err)
        json.NewEncoder(w).Encode("Error")
        return
    }

    // log.Printf("CreateHost(): obj = %v", obj)
    // log.Printf("CreateHost(): workspace = %v", obj["workspace"])
    // log.Printf("CreateHost(): workspace_id = %v", obj["workspace_id"])
    // log.Printf("CreateHost(): host = %v", obj["host"])
    // log.Printf("CreateHost(): name = %v", obj["name"])
    // log.Printf("CreateHost(): state = %v", obj["state"])

    // HACK HACK: Ruby API accepts workspace name, this is only a simple workaround for testing
    if obj["workspace"] == "default" {
        obj["workspace_id"] = 1
        log.Printf("CreateHost(): set workspace_id = %v", obj["workspace_id"])
    }

    var id null.Int
    row := DB.QueryRow("INSERT INTO hosts(workspace_id, address, name, state) VALUES($1, $2, $3, $4) RETURNING id", obj["workspace_id"], obj["host"], obj["name"], obj["state"])
    log.Printf("CreateHost(): row = %v\n", row)
    err := row.Scan(&id)
    log.Printf("CreateHost(): err = %v\n", err)

    response := make(map[string]interface{})
    switch {
    case err == sql.ErrNoRows || err != nil:
        errorMsg := fmt.Sprintf("Error creating host: %s", err)
        log.Printf(errorMsg)
        error := map[string]interface{}{
            "code": 500,
            "message": errorMsg,
        }
        response["error"] = error
    default:
        log.Printf("Host created with ID = %v", id)
        host := map[string]interface{}{
            "id": id,
            "host": obj["host"],
            "name": obj["name"],
            "state": obj["state"],
        }
        response["data"] = host
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(response)
}

func DeleteHosts(w http.ResponseWriter, r *http.Request) {
    var obj map[string]interface{}
    if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
        // handle error
        log.Fatal(err)
        json.NewEncoder(w).Encode("Error")
        return
    }

    log.Printf("DeleteHosts(): obj = %v", obj)

    ids := obj["ids"].([]interface{})
    log.Printf("DeleteHosts(): ids = %v", ids)

    var hosts []map[string]interface{}
    if ids != nil {
        for _, id := range ids {
            log.Printf("DeleteHosts(): deleting host with ID = %v", id)
            stmt, err := DB.Prepare("DELETE from hosts WHERE id=$1")
            log.Printf("DeleteHosts(): stmt = %v\n", stmt)
            checkErr(err, false)
            res, err := stmt.Exec(id)
            log.Printf("DeleteHosts(): res = %v\n", res)
            checkErr(err, false)
            affected, err := res.RowsAffected()
            checkErr(err, false)
            log.Printf("DeleteHosts(): rows affected = %v\n", affected)

            host := map[string]interface{}{
                "id": id,
            }
            hosts = append(hosts, host)
        }
    } else {
        log.Printf("ids is not an array: %v", obj["ids"])
    }

    w.WriteHeader(http.StatusOK)
    response := map[string]interface{}{
        "data": hosts,
    }
    json.NewEncoder(w).Encode(response)
}

func DeleteHost(w http.ResponseWriter, r *http.Request) {
    // TODO: implement
    w.WriteHeader(http.StatusOK)
    response := map[string]interface{}{
        "data": nil,
    }
    json.NewEncoder(w).Encode(response)
}

func UpdateHost(w http.ResponseWriter, r *http.Request) {
    vars := mux.Vars(r)
    fmt.Println("UpdateHost(): vars:", vars)
    fmt.Println("UpdateHost(): id:", vars["id"])

    var obj map[string]interface{}
    if err := json.NewDecoder(r.Body).Decode(&obj); err != nil {
        // handle error
        log.Fatal(err)
        json.NewEncoder(w).Encode("Error")
        return
    }

    log.Printf("UpdateHost(): obj = %v", obj)
    log.Printf("UpdateHost(): comments = %v", obj["comments"])

    // row := DB.QueryRow("UPDATE hosts SET (comments) = ($2) WHERE id=$1", vars["id"], obj["comments"])
    // log.Printf("UpdateHost(): row = %v\n", row)
    // err := row.Scan(&id)
    // log.Printf("UpdateHost(): err = %v\n", err)
    stmt, err := DB.Prepare("UPDATE hosts SET comments=$1 WHERE id=$2")
    log.Printf("UpdateHost(): stmt = %v\n", stmt)
    checkErr(err, false)
    res, err := stmt.Exec(obj["comments"], vars["id"])
    log.Printf("UpdateHost(): res = %v\n", res)
    checkErr(err, false)
    affected, err := res.RowsAffected()
    checkErr(err, false)
    log.Printf("UpdateHost(): rows affected = %v\n", affected)

    response := make(map[string]interface{})
    switch {
    case err == sql.ErrNoRows || err != nil:
        errorMsg := fmt.Sprintf("Error updating host: %s", err)
        log.Printf(errorMsg)
        error := map[string]interface{}{
            "code": 500,
            "message": errorMsg,
        }
        response["error"] = error
    default:
        log.Printf("Host ID %v updated", vars["id"])
        host := map[string]interface{}{
            "id": vars["id"],
        }
        response["data"] = host
    }

    w.WriteHeader(http.StatusOK)
    json.NewEncoder(w).Encode(response)
}

func checkErr(err error, panicOnError bool) {
    if err != nil {
        if panicOnError {
            panic(err)
        } else {
            fmt.Println("checkErr(): err", err)
        }
    }
}

func logDBStats(stats sql.DBStats) {
    log.Printf("init(): DBStats MaxOpenConnections=%v", stats.MaxOpenConnections)
    log.Printf("init(): DBStats OpenConnections=%v", stats.OpenConnections)
    log.Printf("init(): DBStats InUse=%v", stats.InUse)
    log.Printf("init(): DBStats Idle=%v", stats.Idle)
    log.Printf("init(): DBStats WaitCount=%v", stats.WaitCount)
    log.Printf("init(): DBStats WaitDuration=%v", stats.WaitDuration)
    log.Printf("init(): DBStats MaxIdleClosed=%v", stats.MaxIdleClosed)
    log.Printf("init(): DBStats MaxLifetimeClosed=%v", stats.MaxLifetimeClosed)
}

func main() {
    router := mux.NewRouter()
    // hostsRouter = router.PathPrefix("/api/v1/hosts/").Subrouter()

    router.HandleFunc("/health", HealthCheck).Methods("GET")
    router.HandleFunc("/dbstats", GetDBStats).Methods("GET")

    router.HandleFunc("/api/v1/hosts", GetHosts).Methods("GET")
    router.HandleFunc("/api/v1/hosts/{id:[0-9]+}", GetHost).Methods("GET")

    router.HandleFunc("/api/v1/hosts", CreateHost).Methods("POST")

    router.HandleFunc("/api/v1/hosts", DeleteHosts).Methods("DELETE")
    router.HandleFunc("/api/v1/hosts/{id:[0-9]+}", DeleteHost).Methods("DELETE")

    router.HandleFunc("/api/v1/hosts/{id:[0-9]+}", UpdateHost).Methods("PUT")

    fmt.Println("Starting server...")
    // Bind to a port and pass our router in
    log.Fatal(http.ListenAndServe(":9090", router))
}
