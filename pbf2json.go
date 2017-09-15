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
    // os.Exit(1)

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
                id, data := formatLevelDBNode(v)
                cacheQueue(batch, id, data)
                if batch.Len() > config.BatchSize {
                    cacheFlush(db, batch)
                }

                // ----------------
                // handle tags
                // ----------------

                if v.Tags = containsValidTags(v.Tags, config.Tags); v.Tags != nil {
                    onNode(v)
                }

            case *osmpbf.Way:

                // flush outstanding batches
                if batch.Len() > 1 {
                    cacheFlush(db, batch)
                }

                wc++

                if v.Tags = containsValidTags(v.Tags, config.Tags); v.Tags != nil {

                    // special treatment for buildings
                    _, isBuilding := v.Tags["building"]

                    // lookup from leveldb
                    latlons := cacheLookup(db, v)

                    // skip ways which fail to denormalize
                    if latlons == nil {
                       break
                    }

                    var centroid map[string]string

                    if isBuilding {
                       centroid = entranceLookup(db, v)
                    }

                    if centroid == nil {
                        centroid = computeCentroid(latlons)
                    }
                    onWay(v, latlons, centroid)
                }

            case *osmpbf.Relation:

                if batch.Len() > 1 {
                    cacheFlush(db, batch)
                }

                // inc count
                rc++

                if v.Tags = containsValidTags(v.Tags, config.Tags); v.Tags != nil {
                   onRelation(v)
                }

            default:

                log.Fatalf("unknown type %T\n", v)

            }
        }
    }

    // fmt.Printf("Nodes: %d, Ways: %d, Relations: %d\n", nc, wc, rc)
}

type jsonNode struct {
    ID   int64             `json:"id"`
    Type string            `json:"type"`
    Lat  float64           `json:"lat"`
    Lon  float64           `json:"lon"`
    Tags map[string]string `json:"tags"`
}

func onNode(node *osmpbf.Node) {
    marshall := jsonNode{node.ID, "node", node.Lat, node.Lon, node.Tags}
    json, _ := json.Marshal(marshall)
    fmt.Println(string(json))
}

type jsonWay struct {
    ID   int64             `json:"id"`
    Type string            `json:"type"`
    Tags map[string]string `json:"tags"`
    Centroid map[string]string   `json:"centroid"`
    Nodes    []map[string]string `json:"nodes"`
}

func onWay(way *osmpbf.Way, latlons []map[string]string, centroid map[string]string) {
    marshall := jsonWay{way.ID, "way", way.Tags, centroid, latlons}
    json, _ := json.Marshal(marshall)
    fmt.Println(string(json))
}

type JsonRelation struct {
    ID        int64               `json:"id"`
    Type      string              `json:"type"`
    Tags      map[string]string   `json:"tags"`
    Members   []osmpbf.Member     `json:"members"`
}

func onRelation(rel *osmpbf.Relation){
    marshall := JsonRelation{ rel.ID, "relation", rel.Tags, rel.Members }
    json, _ := json.Marshal(marshall)
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

func cacheLookup(db *leveldb.DB, way *osmpbf.Way) ([]map[string]string) {

    var container []map[string]string

    for _, each := range way.NodeIDs {
        node := cacheFetch(db, each)
        if node == nil {
           log.Println("denormalize failed for way:", way.ID, "node not found:", each)
           return nil
        }
        latlon := nodeLatLon(node);
        container = append(container, latlon)
    }

    return container
}

func entranceLookup(db *leveldb.DB, way *osmpbf.Way) map[string]string {

     var latlon map[string]string

     for _, each := range way.NodeIDs {
        node := cacheFetch(db, each)

        if node == nil {
           return nil
        }
        if val, ok := node.Tags["entrance"]; ok {
           if val == "main" {
              return nodeLatLon(node); // use first detected main entrance
           }
           if val == "yes" {
              latlon = nodeLatLon(node);
              // store found entrance but keep on looking for a main entrance
           }
        }
    }

    return latlon;
}

func cacheFetch(db *leveldb.DB, ID int64) *jsonNode {

    stringid := strconv.FormatInt(ID, 10)

    data, err := db.Get([]byte(stringid), nil)
    if err != nil {
       return nil
    }

    var obj map[string]interface{}
    if err := json.Unmarshal(data, &obj); err != nil {
       return nil
    }

    nodetype, ok := obj["type"].(string)

    if ok && nodetype == "node" {
       tags := make(map[string]string)
       _tags, ok := obj["tags"].(map[string]interface{});
       if ok {
          for k, v := range _tags {
              tags[k], _  = v.(string)
          }
       }
       return &jsonNode{ID, "node", obj["lat"].(float64), obj["lon"].(float64), tags}
    }

    return nil
}


func formatLevelDBNode(node *osmpbf.Node) (id string, val []byte) {

    stringid := strconv.FormatInt(node.ID, 10)
    var bufval bytes.Buffer

    marshall := jsonNode{node.ID, "node", node.Lat, node.Lon, node.Tags}
    json, _ := json.Marshal(marshall)

    bufval.WriteString(string(json))
    byteval := []byte(bufval.String())

    return stringid, byteval
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
func containsValidTags(tags map[string]string, group map[string][]string) map[string]string {
     if hasTags(tags) {
        tags = trimTags(tags)

        for _, list := range group {
            if matchTagsAgainstCompulsoryTagList(tags, list) {
               return tags
            }
        }
    }
    return nil
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
func computeCentroid(latlons []map[string]string) map[string]string {

    // convert lat/lon map to geo.PointSet
    points := geo.NewPointSet()
    for _, each := range latlons {
        var lon, _ = strconv.ParseFloat(each["lon"], 64)
        var lat, _ = strconv.ParseFloat(each["lat"], 64)
        points.Push(geo.NewPoint(lon, lat))
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
    var centroid = make(map[string]string)
    centroid["lat"] = strconv.FormatFloat(compute.Lat(), 'f', 6, 64)
    centroid["lon"] = strconv.FormatFloat(compute.Lng(), 'f', 6, 64)

    return centroid
}

func nodeLatLon(node *jsonNode) map[string]string {
      latlon := make(map[string]string)
      latlon["lat"] = strconv.FormatFloat(node.Lat, 'f', 6, 64)
      latlon["lon"] = strconv.FormatFloat(node.Lon, 'f', 6, 64)

      return latlon
}