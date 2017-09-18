package main

import "encoding/json"
import "fmt"
import "flag"
import "bytes"
import "os"
import "log"
import "io"

import "runtime"
import "strings"
import "strconv"
import "github.com/qedus/osmpbf"
import "github.com/syndtr/goleveldb/leveldb"
import "github.com/paulmach/go.geo"

type settings struct {
    PbfPath    string
    LevedbPath string
    Tags       map[string][]string
    BatchSize  int
}

type jsonNode struct {
    ID   int64             `json:"id"`
    Type string            `json:"type"`
    Lat  float64           `json:"lat"`
    Lon  float64           `json:"lon"`
    Tags map[string]string `json:"tags"`
}

type jsonWay struct {
    ID   int64             `json:"id"`
    Type string            `json:"type"`
    Tags map[string]string `json:"tags"`
    Centroid map[string]float64 `json:"centroid"`
    Nodes []map[string]float64  `json:"nodes"`
}

type jsonRelation struct {
    ID        int64               `json:"id"`
    Type      string              `json:"type"`
    Tags      map[string]string   `json:"tags"`
    Centroid  map[string]float64  `json:"centroid"`
}


func getSettings() settings {

    // command line flags
    leveldbPath := flag.String("leveldb", "/tmp", "path to leveldb directory")
    tagList := flag.String("tags", "", "comma-separated list of valid tags, group AND conditions with a +")
    batchSize := flag.Int("batch", 50000, "batch leveldb writes in batches of this size")

    flag.Parse()
    args := flag.Args()

    if len(args) < 1 {
        log.Fatal("invalid args, you must specify a PBF file")
    }

    // invalid tags
    if len(*tagList) < 1 {
        log.Fatal("Nothing to do, you must specify tags to match against")
    }

    // parse tag conditions
    conditions := make(map[string][]string)
    for _, group := range strings.Split(*tagList, ",") {
        conditions[group] = strings.Split(group, "+")
    }

    // fmt.Print(conditions, len(conditions))
    // os.exit(1)

    return settings{args[0], *leveldbPath, conditions, *batchSize}
}

func main() {

    // configuration
    config := getSettings()

    // open pbf file
    file := openFile(config.PbfPath)
    defer file.Close()

    decoder := osmpbf.NewDecoder(file)
    err := decoder.Start(runtime.GOMAXPROCS(-1)) // use several goroutines for faster decoding
    if err != nil {
        log.Fatal(err)
    }

    db := openLevelDB(config.LevedbPath)
    defer db.Close()

    run(decoder, db, config)
}

func run(d *osmpbf.Decoder, db *leveldb.DB, config settings) {

    batch := new(leveldb.Batch)

    var nc, wc, rc uint64

    // NOTE: this logic expects that parser outputs all nodes
    // before ways and all ways before relations
    prevtype := "node"

    for {
        if v, err := d.Decode(); err == io.EOF {
            break
        } else if err != nil {
            log.Fatal(err)
        } else {

            switch v := v.(type) {

            case *osmpbf.Node:

                // inc count
                nc++

                // ----------------
                // write to leveldb
                // ----------------

                // write in batches
                id, data, jNode := formatNode(v)
                cacheQueue(batch, id, data)
                if batch.Len() > config.BatchSize {
                    cacheFlush(db, batch)
                }

                if containsValidTags(jNode.Tags, config.Tags) {
                    printJson(jNode)
                }

            case *osmpbf.Way:

                if prevtype != "way" {
                   prevtype = "way"
                   if batch.Len() > 1 {
                      // flush outstanding node batches
                      cacheFlush(db, batch)
                   }
                }
                wc++

                id, data, jWay := formatWay(v, db)
                if data != nil { // valid entry
                    cacheQueue(batch, id, data)
                    if batch.Len() > config.BatchSize {
                       cacheFlush(db, batch)
                    }

                    if containsValidTags(jWay.Tags, config.Tags) {
                       printJson(jWay)
                    }
                }

            case *osmpbf.Relation:

                if batch.Len() > 1 {
                    cacheFlush(db, batch)
                }
                rc++

                if containsValidTags(v.Tags, config.Tags) {
                   _, _, jRel := formatRelation(v, db)
                   if jRel != nil {
                      printJson(jRel)
                   }
                }

            default:

                log.Fatalf("unknown type %T\n", v)

            }
        }
    }

    // fmt.Printf("Nodes: %d, Ways: %d, Relations: %d\n", nc, wc, rc)
}


func printJson(v interface{}) {
    json, _ := json.Marshal(v)
    fmt.Println(string(json))
}

// write to leveldb immediately
func cacheStore(db *leveldb.DB, id string, val []byte) {
    err := db.Put([]byte(id), []byte(val), nil)
    if err != nil {
        log.Fatal(err)
    }
}

// queue a leveldb write in a batch
func cacheQueue(batch *leveldb.Batch, id string, val []byte) {
    batch.Put([]byte(id), []byte(val))
}

// flush a leveldb batch to database and reset batch to 0
func cacheFlush(db *leveldb.DB, batch *leveldb.Batch) {
    err := db.Write(batch, nil)
    if err != nil {
        log.Fatal(err)
    }
    batch.Reset()
}

func cacheLookup(db *leveldb.DB, way *osmpbf.Way) ([]map[string]float64) {

    var container []map[string]float64

    for _, each := range way.NodeIDs {
        node, _ := cacheFetch(db, each).(*jsonNode)
        if node == nil {
           // log.Println("denormalize failed for way:", way.ID, "node not found:", each)
           return nil
        }
        latlon := nodeLatLon(node);
        container = append(container, latlon)
    }

    return container
}

func entranceLookup(db *leveldb.DB, way *osmpbf.Way) (location map[string]float64, entranceType string) {
     var latlon map[string]float64
     eType := ""

     for _, each := range way.NodeIDs {
        node, _ := cacheFetch(db, each).(*jsonNode)
        if node == nil {
           return nil, eType // bad reference, skip
        }

        val, _type := entranceLatLon(node)

        if _type == "mainEntrance" {
            return val, _type // use first detected main entrance
        }
        if _type == "entrance" {
           latlon = val
           eType = _type
           // store found entrance but keep on looking for a main entrance
        }
    }
    return latlon, eType;
}

func cacheFetch(db *leveldb.DB, ID int64) interface{} {

    stringid := strconv.FormatInt(ID, 10)

    data, err := db.Get([]byte(stringid), nil)
    if err != nil {
       return nil
    }

    var obj map[string]interface{}
    if err := json.Unmarshal(data, &obj); err != nil {
       return nil
    }

    // now when the type is known, unmarshal again to avoid manual casting
    nodetype, _ := obj["type"].(string)
    if nodetype == "node" {
       jNode := jsonNode{}
       json.Unmarshal(data, &jNode)
       return &jNode
    }
    if nodetype == "way" {
       jWay := jsonWay{}
       json.Unmarshal(data, &jWay)
       return &jWay
    }

    return nil
}


func formatNode(node *osmpbf.Node) (id string, val []byte, jnode *jsonNode) {

    stringid := strconv.FormatInt(node.ID, 10)
    var bufval bytes.Buffer

    jNode := jsonNode{node.ID, "node", node.Lat, node.Lon, trimTags(node.Tags)}
    json, _ := json.Marshal(jNode)

    bufval.WriteString(string(json))
    byteval := []byte(bufval.String())

    return stringid, byteval, &jNode
}

func formatWay(way *osmpbf.Way, db *leveldb.DB) (id string, val []byte, jway *jsonWay) {

    stringid := strconv.FormatInt(way.ID, 10)
    var bufval bytes.Buffer

    // special treatment for buildings
    _, isBuilding := way.Tags["building"]

    // lookup from leveldb
    latlons := cacheLookup(db, way)

    // skip ways which fail to denormalize
    if latlons == nil {
        return stringid, nil, nil
    }
    var centroid map[string]float64
    var centroidType string
    if isBuilding {
        centroid, centroidType = entranceLookup(db, way)
    }

    if centroidType == "" {
        centroid = computeCentroid(latlons)
        centroidType = "average"
    }
    way.Tags["_centroidType"] = centroidType;
    jWay := jsonWay{way.ID, "way", trimTags(way.Tags), centroid, latlons}
    json, _ := json.Marshal(jWay)

    bufval.WriteString(string(json))
    byteval := []byte(bufval.String())

    return stringid, byteval, &jWay
}

func formatRelation(relation *osmpbf.Relation, db *leveldb.DB) (id string, val []byte, rel *jsonRelation) {

    stringid := strconv.FormatInt(relation.ID, 10)
    var bufval bytes.Buffer

    var latlons []map[string]float64
    var centroid map[string]float64

    var entranceFound bool
    for _, each := range relation.Members {
        entity := cacheFetch(db, each.ID);
        way, ok := entity.(*jsonWay)
        if ok {
            if cType, ok := way.Tags["_centroidType"]; ok {
                entranceFound = true
                centroid = way.Centroid
                if cType == "mainEntrance" {
                    break // use first detected main entrance
                }
            } else {
                latlons = append(latlons, way.Centroid)
            }
        } else {
            node, ok2 := entity.(*jsonNode)
            if ok2 {

                if val, _type := entranceLatLon(node); _type != "" {
                    entranceFound = true
                    centroid = val
                    if _type == "mainEntrance" {
                        break
                    }
                } else {
                    latlons = append(latlons, nodeLatLon(node))
                }
            } else {
                // log.Println("denormalize failed for relation:", relation.ID, "member not found:", each.ID)
                return stringid, nil, nil
            }
        }
    }

    if !entranceFound {
        if len(latlons) == 0 {
           log.Println("Skipping relation without location: ", relation.ID)
           return stringid, nil, nil
        }
        centroid = computeCentroid(latlons)
    }

    jRelation := jsonRelation{relation.ID, "relation", trimTags(relation.Tags), centroid}
    json, _ := json.Marshal(jRelation)

    bufval.WriteString(string(json))
    byteval := []byte(bufval.String())

    return stringid, byteval, &jRelation
}

func openFile(filename string) *os.File {
    // no file specified
    if len(filename) < 1 {
        log.Fatal("invalid file: you must specify a pbf path as arg[1]")
    }
    // try to open the file
    file, err := os.Open(filename)
    if err != nil {
        log.Fatal(err)
    }
    return file
}

func openLevelDB(path string) *leveldb.DB {
    // try to open the db
    db, err := leveldb.OpenFile(path, nil)
    if err != nil {
        log.Fatal(err)
    }
    return db
}

// extract all keys to array
// keys := []string{}
// for k := range v.Tags {
//     keys = append(keys, k)
// }

// check tags contain features from a whitelist
func matchTagsAgainstCompulsoryTagList(tags map[string]string, tagList []string) bool {
    for _, name := range tagList {

        feature := strings.Split(name, "~")
        foundVal, foundKey := tags[feature[0]]

        // key check
        if !foundKey {
            return false
        }

        // value check
        if len(feature) > 1 {
            if foundVal != feature[1] {
                return false
            }
        }
    }

    return true
}

// check tags contain features from a groups of whitelists
func containsValidTags(tags map[string]string, group map[string][]string) bool {
     if hasTags(tags) {
        for _, list := range group {
            if matchTagsAgainstCompulsoryTagList(tags, list) {
               return true
            }
        }
    }
    return false
}


// trim leading/trailing spaces from keys and values
func trimTags(tags map[string]string) map[string]string {
    trimmed := make(map[string]string)
    for k, v := range tags {
        trimmed[strings.TrimSpace(k)] = strings.TrimSpace(v)
    }
    return trimmed
}

// check if a tag list is empty or not
func hasTags(tags map[string]string) bool {
    n := len(tags)
    if n == 0 {
        return false
    }
    return true
}

// compute the centroid of a way
func computeCentroid(latlons []map[string]float64) map[string]float64 {

    // convert lat/lon map to geo.PointSet
    points := geo.NewPointSet()
    for _, each := range latlons {
        points.Push(geo.NewPoint(each["lon"], each["lat"]))
    }

    // determine if the way is a closed centroid or a linestring
    // by comparing first and last coordinates.
    isClosed := false
    if points.Length() > 2 {
        isClosed = points.First().Equals(points.Last())
    }

    // compute the centroid using one of two different algorithms
    var compute *geo.Point
    if isClosed {
        compute = GetPolygonCentroid(points)
    } else {
        compute = GetLineCentroid(points)
    }

    // return point as lat/lon map
    var centroid = make(map[string]float64)
    centroid["lat"] = compute.Lat()
    centroid["lon"] = compute.Lng()

    return centroid
}

func entranceLatLon(node *jsonNode) (latlon map[string]float64, entranceType string) {

    if val, ok := node.Tags["entrance"]; ok {
        if val == "main" {
            return nodeLatLon(node), "mainEntrance"
        }
        if val == "yes" {
           return nodeLatLon(node), "entrance"
        }
    }
    return nil, ""
}

func nodeLatLon(node *jsonNode) map[string]float64 {
      latlon := make(map[string]float64)
      latlon["lat"] = node.Lat
      latlon["lon"] = node.Lon

      return latlon
}