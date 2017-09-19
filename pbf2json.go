package main

import "encoding/json"
import "fmt"
import "flag"
import "bytes"
import "os"
import "log"
import "io"
import "path/filepath"

import "runtime"
import "strings"
import "strconv"
import "github.com/qedus/osmpbf"
import "github.com/syndtr/goleveldb/leveldb"
import "github.com/paulmach/go.geo"

const translateAddresses = true // disable if no multilang addresses are desired

type Point struct {
    Lat  float64 `json:"lat"`
    Lon  float64 `json:"lon"`
}

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
    Centroid Point         `json:"centroid"`
    Points []Point         `json:"points"`
//    Min Point              `json:"min"`
//    Max Point              `json:"max"`
}

type jsonRelation struct {
    ID        int64               `json:"id"`
    Type      string              `json:"type"`
    Tags      map[string]string   `json:"tags"`
    Centroid  Point               `json:"centroid"`
//    Min Point                     `json:"min"`
//    Max Point                     `json:"max"`
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

    return settings{args[0], *leveldbPath, conditions, *batchSize}
}

func createDecoder(file *os.File) *osmpbf.Decoder {
    decoder := osmpbf.NewDecoder(file)
    err := decoder.Start(runtime.GOMAXPROCS(-1)) // use several goroutines for faster decoding
    if err != nil {
        log.Fatal(err)
        os.Exit(1)
    }
    return decoder
}

func main() {

    // configuration
    config := getSettings()

    config.LevedbPath = filepath.Join(config.LevedbPath, "leveldb")
    os.MkdirAll(config.LevedbPath, os.ModePerm)

    refmap := make(map[int64]bool)         // these ids are needed by OSM reference
    dictionaryIds := make(map[int64]bool)  // these ids may be needed for translation purposes
    validIds := make(map[int64]bool)       // these ids will be outputted as matching items
    dictionary := make(map[string][]int64) // translation map: name -> IDs of matching cached items

    // open pbf file
    file := openFile(config.PbfPath)
    defer file.Close()

    // pass 1: analyze and collect references
    decoder := createDecoder(file)
    collectRefs(decoder, config, refmap, dictionaryIds, dictionary)

    file.Seek(0,0)

    // pass 2: create cache for quick random access
    decoder = createDecoder(file)
    db := openLevelDB(config.LevedbPath)
    defer db.Close()
    createCache(decoder, config, refmap, dictionaryIds, dictionary, validIds, db)

    file.Seek(0,0)

    // pass 3: output items that match tag selection
    decoder = createDecoder(file)
    outputValidEntries(decoder, config, refmap, dictionary, validIds, db)

    db.Close()
    os.RemoveAll(config.LevedbPath)
}

// look in advance which nodes are referred by ways and  relations,
// and which ways are referred by relations
// Then, at second parsing stage, we need to cache only relevant items
func collectRefs(d *osmpbf.Decoder, config settings, refmap map[int64]bool, dictionaryIds map[int64]bool, dictionary map[string][]int64) {
    for {
        if v, err := d.Decode(); err == io.EOF {
            break
        } else if err != nil {
            log.Fatal(err)
        } else {

            switch v := v.(type) {

            case *osmpbf.Way:
                tags, ok := containsValidTags(v.Tags, config.Tags)
                toDict := toStreetDictionary(v.ID, tags, dictionaryIds, dictionary)
                if ok || toDict {
                    for _, each := range v.NodeIDs {
                        refmap[each] = true
                    }
                }

            case *osmpbf.Relation:
                tags, ok := containsValidTags(v.Tags, config.Tags)
                toDict := toStreetDictionary(v.ID, tags, dictionaryIds, dictionary)
                if ok || toDict {
                    for _, each := range v.Members {
                        refmap[each.ID] = true
                    }
                }

            default:
                // nop
            }
        }
    }
    fmt.Println("\n##### Collected refs")
}

func createCache(d *osmpbf.Decoder, config settings, refmap map[int64]bool, dictionaryIds map[int64]bool,
    dictionary map[string][]int64, validIds map[int64]bool, db *leveldb.DB) {

    batch := new(leveldb.Batch)

    var nc, wc, rc uint64
    var valid bool

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
                v.Tags, valid = containsValidTags(v.Tags, config.Tags)
                if valid || refmap[v.ID] {
                   id, data, _ := formatNode(v)
                   cacheQueue(batch, id, data)
                   if batch.Len() > config.BatchSize {
                       cacheFlush(db, batch)
                   }
                   if valid {
                       validIds[v.ID] = true
                       nc++
                   }
                }

            case *osmpbf.Way:

                if prevtype != "way" {
                   prevtype = "way"
                   if batch.Len() > 1 {
                      cacheFlush(db, batch)
                   }
                }

                v.Tags, valid = containsValidTags(v.Tags, config.Tags)
                if valid || refmap[v.ID] || dictionaryIds[v.ID] {
                    id, data, _ := formatWay(v, db)
                    if data != nil { // valid entry
                        cacheQueue(batch, id, data)
                        if batch.Len() > config.BatchSize {
                            cacheFlush(db, batch)
                        }
                        if valid {
                            validIds[v.ID] = true
                            wc++
                        }
                    }
                }

            case *osmpbf.Relation:

                if prevtype != "relation" {
                   prevtype = "relation"
                   if batch.Len() > 1 {
                      cacheFlush(db, batch)
                   }
                }

                v.Tags, valid = containsValidTags(v.Tags, config.Tags)
                if valid || dictionaryIds[v.ID] {
                    id, data, _ := formatRelation(v, db)
                    if data != nil {
                        cacheQueue(batch, id, data)
                        if batch.Len() > config.BatchSize {
                            cacheFlush(db, batch)
                        }
                        if valid {
                            validIds[v.ID] = true
                            rc++
                        }
                   }
                }

            default:
                log.Fatalf("unknown type %T\n", v)

            }
        }
    }

    fmt.Printf("##### \nCaching done. valid Nodes: %d, Ways: %d, Relations: %d\n", nc, wc, rc)
}


func outputValidEntries(d *osmpbf.Decoder, config settings, refmap map[int64]bool,
    dictionary map[string][]int64, validIds map[int64]bool, db *leveldb.DB) {

    for {
        if v, err := d.Decode(); err == io.EOF {
            break
        } else if err != nil {
            log.Fatal(err)
        } else {
            switch v := v.(type) {
            case *osmpbf.Node:
                if _, ok := validIds[v.ID]; !ok {
                    continue
                }
                node, _ := cacheFetch(db, v.ID).(*jsonNode)
                translateAddress(node.Tags, dictionary, db)
                printJson(node)

            case *osmpbf.Way:
                if _, ok := validIds[v.ID]; !ok {
                    continue
                }
                way, _ := cacheFetch(db, v.ID).(*jsonWay)
                translateAddress(way.Tags, dictionary, db)
                printJson(way)

            case *osmpbf.Relation:
                if _, ok := validIds[v.ID]; !ok {
                    continue
                }
                rel, _ := cacheFetch(db, v.ID).(*jsonRelation)
                translateAddress(rel.Tags, dictionary, db)
                printJson(rel)
            }
        }
    }
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

func geometryLookup(db *leveldb.DB, way *osmpbf.Way) ([]Point) {

    var container []Point

    for _, each := range way.NodeIDs {
        node, _ := cacheFetch(db, each).(*jsonNode)
        if node == nil {
           // log.Println("denormalize failed for way:", way.ID, "node not found:", each)
           return nil
        }
        container = append(container, Point{Lat: node.Lat, Lon: node.Lon})
    }

    return container
}

func entranceLookup(db *leveldb.DB, way *osmpbf.Way) (location Point, entranceType string) {
     var foundLocation Point
     eType := ""

     for _, each := range way.NodeIDs {
        node, _ := cacheFetch(db, each).(*jsonNode)
        if node == nil {
           return location, eType // bad reference, skip
        }

        val, _type := entranceLocation(node)

        if _type == "mainEntrance" {
            return val, _type // use first detected main entrance
        }
        if _type == "entrance" {
           foundLocation = val
           eType = _type
           // store found entrance but keep on looking for a main entrance
        }
    }
    return foundLocation, eType;
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

    jNode := jsonNode{node.ID, "node", node.Lat, node.Lon, node.Tags}
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
    points := geometryLookup(db, way)

    // skip ways which fail to denormalize
    if points == nil {
        return stringid, nil, nil
    }
    var centroid Point
    var centroidType string
    if isBuilding {
        centroid, centroidType = entranceLookup(db, way)
    }

    if centroidType == "" {
        centroid = computeCentroid(points)
        centroidType = "average"
    }
    way.Tags["_centroidType"] = centroidType;
    jWay := jsonWay{way.ID, "way", way.Tags, centroid, points}
    json, _ := json.Marshal(jWay)

    bufval.WriteString(string(json))
    byteval := []byte(bufval.String())

    return stringid, byteval, &jWay
}

func formatRelation(relation *osmpbf.Relation, db *leveldb.DB) (id string, val []byte, rel *jsonRelation) {

    stringid := strconv.FormatInt(relation.ID, 10)
    var bufval bytes.Buffer

    var points []Point
    var centroid Point

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
                points = append(points, way.Centroid)
            }
        } else {
            node, ok2 := entity.(*jsonNode)
            if ok2 {

                if val, _type := entranceLocation(node); _type != "" {
                    entranceFound = true
                    centroid = val
                    if _type == "mainEntrance" {
                        break
                    }
                } else {
                    points = append(points, Point{Lat:node.Lat, Lon:node.Lon})
                }
            } else {
                // log.Println("denormalize failed for relation:", relation.ID, "member not found:", each.ID)
                return stringid, nil, nil
            }
        }
    }

    if !entranceFound {
        if len(points) == 0 {
           log.Println("Skipping relation without location: ", relation.ID)
           return stringid, nil, nil
        }
        centroid = computeCentroid(points)
    }

    jRelation := jsonRelation{relation.ID, "relation", relation.Tags, centroid}
    json, _ := json.Marshal(jRelation)

    bufval.WriteString(string(json))
    byteval := []byte(bufval.String())

    return stringid, byteval, &jRelation
}

func openFile(filename string) *os.File {
    // no file specified
    if len(filename) < 1 {
        log.Fatal("invalidfile: you must specify a pbf path as arg[1]")
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
func containsValidTags(tags map[string]string, group map[string][]string) (map[string]string,  bool) {
    if hasTags(tags) {
        tags = trimTags(tags)
        for _, list := range group {
            if matchTagsAgainstCompulsoryTagList(tags, list) {
               return tags, true
            }
        }
    }
    return tags, false
}

// check if tags contain features which are useful for address translations
func toStreetDictionary(ID int64, tags map[string]string, dictionaryIds map[int64]bool, dictionary map[string][]int64) bool {
    if translateAddresses && hasTags(tags) {
        if _, ok := tags["highway"]; ok {
            if name, ok2 := tags["name"]; ok2 {
                for k, v := range tags {
                    if strings.Contains(k, "name:") && v != name {
                        dictionaryIds[ID] = true
                    /*    if dictionary[name] == nil {
                            dictionary[name] = make([]int64, 0)
                        } */
                        dictionary[name] = append(dictionary[name], ID)
                        return true
                    }
                }
            }
        }
    }
    return false
}

func translateAddress(tags map[string]string, dictionary map[string][]int64, db *leveldb.DB) {

    if !translateAddresses {
        return
    }
    var streetname, housenumber string
    var ok bool
    if streetname, ok = tags["address:street"]; !ok {
       return
    }
    if housenumber, ok = tags["address:housenumber"]; !ok {
       return
    }

    var tags2 map[string]string

    if translations, ok2 := dictionary[tags["streetname"]]; ok2 {
        for _, id := range translations {
            entity := cacheFetch(db, id)
            if way, ok3 := entity.(*jsonWay); ok3 {
               tags2 = way.Tags
            } else if rel, ok3 := entity.(*jsonRelation); ok3 {
               tags2 = rel.Tags
            } else {
               log.Fatalf("Unexpected translation entity %d", id)
            }
            for k, v := range tags2 {
                if strings.Contains(k, "name:") && streetname != v  {
                    if _, ok = tags[k]; !ok { // name:lang entry not yet in use
                       tags[k] = housenumber + " " + v // Hooray! Translated!
                       fmt.Println("Translated", streetname, tags[k])
                    }
                }
            }
        }
    }
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
func computeCentroid(wayGeometry []Point) Point {

    // convert lat/lon map to geo.PointSet
    points := geo.NewPointSet()
    for _, each := range wayGeometry {
        points.Push(geo.NewPoint(each.Lon, each.Lat))
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

    return Point{Lat: compute.Lat(), Lon: compute.Lng()}
}

func entranceLocation(node *jsonNode) (location Point, entranceType string) {

    if val, ok := node.Tags["entrance"]; ok {
        if val == "main" {
            return Point{Lat: node.Lat, Lon: node.Lon}, "mainEntrance"
        }
        if val == "yes" {
           return Point{Lat: node.Lat, Lon: node.Lon}, "entrance"
        }
    }
    return Point{}, ""
}
