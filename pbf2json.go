package main

import "encoding/json"
import "fmt"
import "flag"
import "bytes"
import "os"
import "log"
import "io"
import "path/filepath"
import "regexp"
import "runtime"
import "strings"
import "strconv"
import "github.com/qedus/osmpbf"
import "github.com/syndtr/goleveldb/leveldb"
import "github.com/paulmach/go.geo"
//import "github.com/davecgh/go-spew/spew"

const streetHitDistance = 0.01 // in wgs coords, about a kilometer
var isAddrRef = regexp.MustCompile(`^[a-zA-Z]{1}([1-9])?$`).MatchString

type Point struct {
    Lat  float64 `json:"lat"`
    Lon  float64 `json:"lon"`
}

type TagValue struct {
     values map[string]bool
     regex *regexp.Regexp
     any bool
}

type TagExp struct {
     regex *regexp.Regexp
     value *TagValue
}

type TagSelector struct {
    tags map[string]*TagValue
    tagex []TagExp
}

type settings struct {
    PbfPath    string
    LevedbPath string
    Tags map[int][]*TagSelector
    BatchSize  int
    names map[string]bool
    highways map[string]bool
}

type jsonNode struct {
    ID   int64             `json:"id"`
    Type string            `json:"type"`
    Lat  float64           `json:"lat"`
    Lon  float64           `json:"lon"`
    Tags map[string]string `json:"tags"`
}

type jsonWayRel struct {
    ID   int64             `json:"id"`
    Type string            `json:"type"`
    Tags map[string]string `json:"tags"`
    Centroid Point         `json:"centroid"`
    BBoxMin  Point         `json:"bbox_min"`
    BBoxMax  Point         `json:"bbox_max"`
}

type cacheId struct {
    ID int64
    Type osmpbf.MemberType
}

type amenityName map[string]string

// dictionary for assigning standard names and translations to a set of entities
var amenityNames = map[string]amenityName {
    "library": { "fi": "Kirjasto", "en":"Library", "sv":"Bibliotek" },
    "fire_station": { "fi":"Paloasema", "en":"Fire station", "sv":"Brandstation" },
    "university": { "fi":"Yliopisto", "en":"University", "sv":"Universitet" },
    "bus_station": { "fi":"Linja-autoasema", "en":"Bus station", "sv":"Busstationen" },
    "hospital": { "fi":"Sairaala", "en":"Hospital", "sv":"Sjukhus" },
    "clinic": { "fi":"Terveyskeskus", "en":"Medical center", "sv":"Hälsocentral" },
    "police": { "fi":"Poliisiasema", "en":"Police station", "sv":"Polisstation" },
    "townhall": { "fi":"Kaupungintalo", "en":"Town hall", "sv":"Stadshus" },
}

type context struct {
    file *os.File

    nodes *leveldb.DB
    ways *leveldb.DB

    // items which are needed because searched items refer to them
    nodeRef map[int64]bool
    wayRef map[int64]bool

    // store time consuming tag comparison results to these maps
    validNodes map[int64]bool
    validWays map[int64]bool
    validRelations map[int64]bool

    // put relations to memory resident map for quick access in resolving cross references
    relations map[int64]*osmpbf.Relation
    formattedRelations map[int64]*jsonWayRel
    pendingRelations map[int64]bool

    // items which are needed in translating address type items to multiple languages
    dictionaryWays map[int64]bool
    dictionaryRelations map[int64]bool

    translations map[string][]cacheId
    streets map[string][]cacheId
    mergedStreets map[int64]*jsonWayRel
    waterways map[string][]cacheId
    mergedWater map[int64]*jsonWayRel

    entrances map[int64]*jsonNode // accurate address points generated runtime

    config *settings
    transcount int64
    fitranscount int64
    amenitycount int64
}

func getSettings() settings {

    // command line flags
    leveldbPath := flag.String("leveldb", "/tmp", "path to leveldb directory")

    tagList := flag.String("tags", "", "comma-separated list of valid tags, group AND conditions with a §")
    batchSize := flag.Int("batch", 50000, "batch leveldb writes in batches of this size")
    names := flag.String("names", "name", "comma-separated list of supported names")
    highways := flag.String("highways", "", "comma-separated list of supported highway values")

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
    groups := make(map[int][]*TagSelector)
    for i, group := range strings.Split(*tagList, ",") { // top level alternatives
        for _, conditions := range strings.Split(group, "§") { // AND combined conditions for a single alternative
            condition := &TagSelector{make(map[string]*TagValue), make([]TagExp, 0)}
            for _, tag := range strings.Split(conditions, "!") { // tag alternatives, combined with OR
                tv := TagValue{make(map[string]bool), nil, false}
                pair := strings.Split(tag, "~") // tag name + optional value(s)
                if len(pair) > 1 {
                   valueDef := pair[1];
                   p1 := strings.LastIndex(valueDef, "#") // regex?
                   if p1 >= 0 {
                      tv.regex = regexp.MustCompile(valueDef[p1+1:])
                   } else {
                      for _, val := range strings.Split(valueDef, ";") {
                          tv.values[val] = true;
                      }
                   }
                } else {
                   tv.any = true
                }
                tname := pair[0];
                // check regex
                pos := strings.LastIndex(tname, "#")
                if pos >= 0 {
                    condition.tagex = append(condition.tagex, TagExp{regexp.MustCompile(tname[pos+1:]), &tv})
                } else {
                    condition.tags[tname] = &tv;
                }
            }
            groups[i] = append(groups[i], condition)
        }
    }
    //spew.Dump(groups)

    nameMap := make(map[string]bool)
    for _, name := range strings.Split(*names, ",") {
        nameMap[name] = true
    }

    var hwMap map[string]bool
    if *highways != "" {
       hwMap := make(map[string]bool)
       for _, val := range strings.Split(*highways, ",") {
           hwMap[val] = true
       }
    }
    return settings{args[0], *leveldbPath, groups, *batchSize, nameMap, hwMap}
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

func (context *context) init() {
    // set up caches
    config := getSettings()

    // open pbf file
    context.file = openFile(config.PbfPath)

    config.LevedbPath = filepath.Join(config.LevedbPath, "leveldb")
    nodePath := filepath.Join(config.LevedbPath, "nodes")
    wayPath := filepath.Join(config.LevedbPath, "ways")

    os.MkdirAll(nodePath, os.ModePerm)
    os.MkdirAll(wayPath, os.ModePerm)

    context.config = &config

    // Actual leveldb caches
    context.nodes = openLevelDB(nodePath)
    context.ways = openLevelDB(wayPath)

    // set up reference maps
    context.nodeRef = make(map[int64]bool) // these ids are needed by OSM references
    context.wayRef = make(map[int64]bool)

    context.validNodes = make(map[int64]bool) // these ids will be outputted as matching items
    context.validWays = make(map[int64]bool)
    context.validRelations = make(map[int64]bool)

    context.relations = make(map[int64]*osmpbf.Relation)
    context.formattedRelations = make(map[int64]*jsonWayRel)
    context.pendingRelations = make(map[int64]bool) // resolve state map to stop infinite recursion

    context.dictionaryWays = make(map[int64]bool) // these ids may be needed for translation purposes
    context.dictionaryRelations = make(map[int64]bool)

    context.translations = make(map[string][]cacheId) // collected translation link map as name -> [cache references]
    context.transcount = 0
    context.fitranscount = 0
    context.amenitycount = 0

    context.streets = make(map[string][]cacheId) // all streets collected here for merge process
    context.mergedStreets = make(map[int64]*jsonWayRel)
    context.entrances = make(map[int64]*jsonNode)

    context.waterways = make(map[string][]cacheId) // for waterway merging
    context.mergedWater = make(map[int64]*jsonWayRel)
}


func (context *context) close() {
    if context.nodes != nil {
        context.nodes.Close()
    }
    if context.ways != nil {
        context.ways.Close()
    }
    if context.file != nil {
       context.file.Close()
    }
    os.RemoveAll(context.config.LevedbPath)
}

func main() {
    var context context

    // configuration
    context.init()
    defer context.close()

    // pass 1: analyze and collect relation references
    decoder := createDecoder(context.file)
    collectRelationRefs(decoder, &context)

    context.file.Seek(0,0)

    // pass 2: analyze and collect way references
    decoder = createDecoder(context.file)
    collectWayRefs(decoder, &context)

    context.file.Seek(0,0)

    // pass 3: create cache for quick random access. Output matching items on the fly.
    decoder = createDecoder(context.file)
    createCache(decoder, &context)

    // merge segmented data
    mergeSegments(&context, context.streets, context.mergedStreets)
    mergeSegments(&context, context.waterways, context.mergedWater)

    // output items that match tag selection
    outputValidEntries(&context)

    println("Translated address point count", context.transcount)
    println("Address translations to fi", context.fitranscount)
    println("Translated amenities", context.amenitycount)

    context.close()
}

// look in advance which items are referred by relations
func collectRelationRefs(d *osmpbf.Decoder, context *context) {
    for {
        if v, err := d.Decode(); err == io.EOF {
            break
        } else if err != nil {
            log.Fatal(err)
        } else {

            switch v := v.(type) {

            case *osmpbf.Relation:
                tags, valid := containsValidTags(v.Tags, context.config.Tags)
                toDictionary(v.ID, osmpbf.RelationType, tags, context.dictionaryRelations, context)
                context.relations[v.ID] = v
                if valid {
                    context.validRelations[v.ID] = true
                }

                // always cache all relations and items referred by them
                for _, each := range v.Members {
                    switch each.Type {
                        case osmpbf.NodeType:
                            context.nodeRef[each.ID] = true

                        case osmpbf.WayType:
                            context.wayRef[each.ID] = true
                    }
                }
            }
        }
    }
}

// look in advance which nodes are referred by ways
func collectWayRefs(d *osmpbf.Decoder, context *context) {
    for {
        if v, err := d.Decode(); err == io.EOF {
            break
        } else if err != nil {
            log.Fatal(err)
        } else {

            switch v := v.(type) {

            case *osmpbf.Way:
                tags, ok := containsValidTags(v.Tags, context.config.Tags)
                toDict := toDictionary(v.ID, osmpbf.WayType, tags, context.dictionaryWays, context)
                if ok || toDict || context.wayRef[v.ID] == true {
                    for _, each := range v.NodeIDs {
                        context.nodeRef[each] = true
                    }
                }
            }
        }
    }
    println("Street dictionary size", len(context.translations))
}

func createCache(d *osmpbf.Decoder, context *context) {

    config := context.config
    batch := new(leveldb.Batch)

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
                if valid || context.nodeRef[v.ID] {
                   id, data, _ := formatNode(v)
                   if data != nil {
                       cacheQueue(batch, id, data)
                       if batch.Len() > config.BatchSize {
                           cacheFlush(context.nodes, batch)
                       }
                       if valid {
                           context.validNodes[v.ID] = true
                       }
                   }
                }

            case *osmpbf.Way:

                if prevtype != "way" {
                   prevtype = "way"
                   if batch.Len() > 1 {
                      cacheFlush(context.nodes, batch)
                   }
                }

                v.Tags, valid = containsValidTags(v.Tags, config.Tags)
                if valid || context.wayRef[v.ID] || context.dictionaryWays[v.ID] {
                    id, data, _ := formatWay(v, context)
                    if data != nil { // valid entry
                        cacheQueue(batch, id, data)
                        if batch.Len() > config.BatchSize {
                            cacheFlush(context.ways, batch)
                        }
                        if valid {
                            context.validWays[v.ID] = true
                        }
                    }
                }

            case *osmpbf.Relation:

               if batch.Len() > 1 {
                   if prevtype == "node" {
                      cacheFlush(context.nodes, batch)
                   } else {
                      cacheFlush(context.ways, batch)
                   }
                }

                if context.validRelations[v.ID] || context.dictionaryRelations[v.ID] {
                   if formatRelation(context.relations[v.ID], context) == nil {
                      delete(context.validRelations, v.ID)
                   }
                }
            }
        }
    }
    if batch.Len() > 0 {
       if prevtype == "node" {
          cacheFlush(context.nodes, batch)
       } else {
          cacheFlush(context.ways, batch)
       }
    }
}

func outputValidEntries(context *context) {

    for id, _ := range context.validNodes {
        node := cacheFetch(context.nodes, id).(*jsonNode)
        translateAddress(node.Tags, &Point{node.Lat, node.Lon}, context)
        printJson(node)
    }
    for id, _ := range context.validWays {
        way := cacheFetch(context.ways, id).(*jsonWayRel)
        if _, ok := way.Tags["highway"]; ok { // streets are outputted separately
            if _, s := context.mergedStreets[id]; s {
               continue
            }
            if xwayOnly(way.Tags, "highway", context.config.Tags) {
               continue
            }
        }
        if _, ok := way.Tags["waterway"]; ok { // waterways are outputted separately
            if _, s := context.mergedWater[id]; s {
               continue
            }
            if xwayOnly(way.Tags, "waterway", context.config.Tags) {
               continue
            }
        }
        translateAddress(way.Tags, &way.Centroid, context)
        printJson(way)
    }
    for id, _ := range context.validRelations {
        relation := context.formattedRelations[id]
        if _, ok := relation.Tags["highway"]; ok {
            if _, s := context.mergedStreets[id]; s {
               continue
            }
            if xwayOnly(relation.Tags, "highway", context.config.Tags) {
               continue
            }
        }
        if _, ok := relation.Tags["waterway"]; ok {
            if _, s := context.mergedWater[id]; s {
               continue
            }
            if xwayOnly(relation.Tags, "waterway", context.config.Tags) {
               continue
            }
        }
        translateAddress(relation.Tags, &relation.Centroid, context)
        printJson(relation)
    }
    for _, street := range context.mergedStreets {
        printJson(street)
    }
    for _, water := range context.mergedWater {
        printJson(water)
    }
    for _, entrance := range context.entrances {
        translateAddress(entrance.Tags, &Point{entrance.Lat, entrance.Lon}, context)
        printJson(entrance)
        // logJson(entrance)
    }
}


func printJson(v interface{}) {
    json, _ := json.Marshal(v)
    fmt.Println(string(json))
}

/*
func logJson(v interface{}) {
    json, _ := json.Marshal(v)
    println(string(json)")
}
*/

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

func collectPoints(db *leveldb.DB, way *osmpbf.Way) ([]Point) {

    var container []Point

    for _, each := range way.NodeIDs {
        node, ok := cacheFetch(db, each).(*jsonNode)
        if !ok {
           return nil
        }
        container = append(container, Point{Lat: node.Lat, Lon: node.Lon})
    }

    return container
}

func validateUnit(tags map[string]string, key string)(result string, isvalid bool) {
    s, hasTag := tags[key]
    if !hasTag {
       return "", false
    }
    s = strings.TrimSpace(s)
    if !isAddrRef(s) {
        return s, false
    }
    if len(s) > 2 { // cut additional flat number part away
        if s[2:3] == " " {
           s = strings.TrimSpace(s[0:2])
        } else if s[1:2] == " " {
           s = strings.TrimSpace(s[0:1])
        } else {
          return s, false
        }
    }
    return s, true
}

func addressMatch(street string, housenumber string, tags map[string]string) bool {
     street2, hasStreet := tags["addr:street"]
     housenumber2, hasNumber := tags["addr:housenumber"]
     if (hasStreet && street != "" && street2 != street) || (hasNumber && housenumber != "" && housenumber2 != housenumber) {
         // println("Skipping mismatching entrance " + street + " " + housenumber + ", " + street2 + " " + housenumber2)
         return false
     }
     return true
}

func entranceLookup(db *leveldb.DB, way *osmpbf.Way, street string, housenumber string, context *context) (location Point, entranceType string) {
     var foundLocation Point
     eType := ""

     for _, each := range way.NodeIDs {
        node, ok := cacheFetch(db, each).(*jsonNode)
        if !ok {
           continue
        }

        val, _type := entranceLocation(node)

        if _type == "" {
           continue
        }

        // large buildings may have several entrances. We must be careful not to use
        //  an entrance point on one street for an address on another street
	if !addressMatch(street, housenumber, node.Tags) {
	    continue // do not assign entrances to wrong streets
        }
	_, hasStreet := node.Tags["addr:street"]
        _, hasNumber := node.Tags["addr:housenumber"]

        if _type == "mainEntrance" {
            foundLocation = val
            eType = _type
            if street == "" { // no need to parse all entrances
                return val, _type // use first detected main entrance
            }
        } else {
            if (eType != "mainEntrance") { // don't overrule main entrance by minor entrance
               // store found entrance but keep on looking for a main entrance
               foundLocation = val
               eType = _type
            }
        }
        if street != "" { // parent entity has valid street address
            ref, hasRef := validateUnit(node.Tags, "ref")
            if !hasRef {
               ref, hasRef = validateUnit(node.Tags, "addr:unit")
            }
            if hasRef {
              if !hasStreet {
                  node.Tags["addr:street"] = street // add missing addr info
              }
              if !hasNumber {
                  node.Tags["addr:housenumber"] = housenumber
              }
              node.Tags["addr:unit"] = ref // use addr:unit to pass staircase/entrance
              context.entrances[node.ID] = node
            }
        }
    }
    return foundLocation, eType
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
       jWay := jsonWayRel{}
       json.Unmarshal(data, &jWay)
       return &jWay
    }

    return nil
}


func formatNode(node *osmpbf.Node) (id string, val []byte, jnode *jsonNode) {

    stringid := strconv.FormatInt(node.ID, 10)
    var bufval bytes.Buffer

    _, hasStreet := node.Tags["addr:street"]
    _, hasHouseNumber := node.Tags["addr:housenumber"]
    hasAddress := hasStreet && hasHouseNumber
    if hasAddress {
      _, hasUnit := validateUnit(node.Tags, "addr:unit")
      if !hasUnit {
        ref, hasRef := validateUnit(node.Tags, "ref")
        if hasRef {
          node.Tags["addr:unit"] = ref // use addr:unit to pass staircase/entrance
        }
      }
    }
    jNode := jsonNode{node.ID, "node", node.Lat, node.Lon, node.Tags}
    json, _ := json.Marshal(jNode)

    bufval.WriteString(string(json))
    byteval := []byte(bufval.String())

    return stringid, byteval, &jNode
}

// expand bbox with a single point
func expandBBox(p, bboxmin, bboxmax *Point) {
     if p.Lat < bboxmin.Lat {
         bboxmin.Lat = p.Lat
     } else if p.Lat > bboxmax.Lat {
         bboxmax.Lat = p.Lat
     }
     if p.Lon < bboxmin.Lon {
         bboxmin.Lon = p.Lon
     } else if p.Lon > bboxmax.Lon {
         bboxmax.Lon = p.Lon
     }
}

// merge 2 bboxes to latter one
func sumBBox(bboxmin1, bboxmax1, bboxmin2, bboxmax2 *Point) {
     if bboxmin1.Lat < bboxmin2.Lat {
         bboxmin2.Lat = bboxmin1.Lat
     }
     if bboxmax1.Lat > bboxmax2.Lat {
         bboxmax2.Lat = bboxmax1.Lat
     }
     if bboxmin1.Lon < bboxmin2.Lon {
         bboxmin2.Lon = bboxmin1.Lon
     }
     if bboxmax1.Lon > bboxmax2.Lon {
         bboxmax2.Lon = bboxmax1.Lon
     }
}

// test
func insideBBox(p, bboxmin, bboxmax *Point) bool {
     return p.Lat >= bboxmin.Lat - streetHitDistance &&
            p.Lat <= bboxmax.Lat + streetHitDistance &&
            p.Lon >= bboxmin.Lon - streetHitDistance &&
            p.Lon <= bboxmax.Lon + streetHitDistance
}

// test
func BBoxIntersects(bboxmin1, bboxmax1, bboxmin2, bboxmax2 *Point) bool {
     if bboxmin1.Lat > bboxmax2.Lat + streetHitDistance ||
        bboxmax1.Lat < bboxmin2.Lat - streetHitDistance ||
        bboxmin1.Lon > bboxmax2.Lon + streetHitDistance ||
        bboxmax1.Lon < bboxmin2.Lon - streetHitDistance {
        return false
     }
     return true
}

func formatWay(way *osmpbf.Way, context *context) (id string, val []byte, jway *jsonWayRel) {

    stringid := strconv.FormatInt(way.ID, 10)
    var bufval bytes.Buffer

    // special treatment for buildings
    _, isBuilding := way.Tags["building"]
    street, hasStreet := way.Tags["addr:street"]
    houseNumber, hasHouseNumber := way.Tags["addr:housenumber"]
    hasAddress := hasStreet && hasHouseNumber
    if !hasAddress {
       street = ""
       houseNumber = ""
    } else {
      _, hasUnit := validateUnit(way.Tags, "addr:unit")
      if !hasUnit {
        ref, hasRef := validateUnit(way.Tags, "ref")
        if hasRef {
          way.Tags["addr:unit"] = ref // use addr:unit to pass staircase/entrance
        }
      }
    }

    // lookup from leveldb
    points := collectPoints(context.nodes, way)

    // skip ways which fail to denormalize
    if points == nil {
        return stringid, nil, nil
    }

    bboxmin := points[0]
    bboxmax := bboxmin

    for _, p := range points {
        expandBBox(&p, &bboxmin, &bboxmax)
    }

    var centroid Point
    var centroidType string
    if isBuilding || hasAddress {
        centroid, centroidType = entranceLookup(context.nodes, way, street, houseNumber, context)
    }

    if centroidType == "" {
        centroid = computeCentroid(points)
        centroidType = "average"
    }
    way.Tags["_centroidType"] = centroidType
    jWay := jsonWayRel{way.ID, "way", way.Tags, centroid, bboxmin, bboxmax}
    json, _ := json.Marshal(jWay)

    bufval.WriteString(string(json))
    byteval := []byte(bufval.String())

    return stringid, byteval, &jWay
}

func formatRelation(relation *osmpbf.Relation, context *context) *jsonWayRel {

    if relation == nil {
       return nil;
    }
    if _rel, ok := context.formattedRelations[relation.ID]; ok {
        return _rel // already done
    }
    if _, ok := context.pendingRelations[relation.ID]; ok {
        return nil // cycle, stop!
    }

    context.pendingRelations[relation.ID] = true // mark to stop cyclic resolving
    defer delete(context.pendingRelations, relation.ID)

    var points []Point
    var centroid Point

    var bboxmin, bboxmax Point
    bbox_init := false

    street, hasStreet := relation.Tags["addr:street"]
    houseNumber, hasHouseNumber := relation.Tags["addr:housenumber"]
    hasAddress := hasStreet && hasHouseNumber
    if !hasAddress {
       street = ""
       houseNumber = ""
    }

    centroidType := ""
    for _, each := range relation.Members {
        switch each.Type {

        case osmpbf.NodeType:
            if node, ok := cacheFetch(context.nodes, each.ID).(*jsonNode); ok {
                p := Point{Lat:node.Lat, Lon:node.Lon}
                if !bbox_init {
                    bboxmin = p
                    bboxmax = p
                    bbox_init = true
                } else {
                    expandBBox(&p, &bboxmin, &bboxmax)
                }
                if val, cType := entranceLocation(node); cType != "" {
		    if addressMatch(street, houseNumber, node.Tags) && (centroidType == "" || cType == "mainEntrance") {
                        centroid = val
                        centroidType = cType
                    } else {
                      points = append(points, p)
                    }
                } else {
                    points = append(points, p)
                }
            } else { // broken ref, skip
                return nil
            }

        case osmpbf.WayType:
            if way, ok := cacheFetch(context.ways, each.ID).(*jsonWayRel); ok {
                if cType, ok := way.Tags["_centroidType"]; ok && cType != "average" {
                    if addressMatch(street, houseNumber, way.Tags) && (centroidType == "" || cType == "mainEntrance") {
                        centroid = way.Centroid
                        centroidType = cType
                    } else {
                        points = append(points, way.Centroid)
               	    }
                } else {
                    points = append(points, way.Centroid)
                }
                if !bbox_init {
                    bboxmin = way.BBoxMin
                    bboxmax = way.BBoxMax
                    bbox_init = true
                } else {
                    sumBBox(&way.BBoxMin, &way.BBoxMax, &bboxmin, &bboxmax)
                }
            } else {
                return nil
            }

        case osmpbf.RelationType:
            var relation *jsonWayRel
            relation = formatRelation(context.relations[each.ID], context) // recurse
            if relation == nil {
                continue
            }

            if cType, ok := relation.Tags["_centroidType"]; ok && cType != "average" {
                if addressMatch(street, houseNumber, relation.Tags) && (centroidType == "" || cType == "mainEntrance") {
                    centroid = relation.Centroid
                    centroidType = cType
                } else {
                    points = append(points, relation.Centroid)
                }
            } else {
                points = append(points, relation.Centroid)
            }
            if !bbox_init {
                bboxmin = relation.BBoxMin
                bboxmax = relation.BBoxMax
                bbox_init = true
            } else {
                sumBBox(&relation.BBoxMin, &relation.BBoxMax, &bboxmin, &bboxmax)
            }
        }
    }

    if centroidType == "" {
        if len(points) == 0 {
           // skip relation if no geometry was found
           return nil
        }
        centroid = computeCentroid(points)
        centroidType = "average"
    }

    relation.Tags["_centroidType"] = centroidType

    jRelation := jsonWayRel{relation.ID, "relation", relation.Tags, centroid, bboxmin, bboxmax}
    context.formattedRelations[relation.ID] = &jRelation

    return &jRelation
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


func testTagVal(val string, tv *TagValue) bool {
     if tv.any == true {
           return true
     }
     if _, ok :=  tv.values[val]; ok {
           return true
     }
     if tv.regex != nil {
           return tv.regex.MatchString(val)
     }
     return false
}


// check tags contain features from a whitelist
func matchTagsAgainstCompulsoryTagList(tags map[string]string, compulsory []*TagSelector) bool {

     // must satisfy - all - conditions of compulsory
     for _, cond := range compulsory {
           found := false
           for tag, val := range tags {
               if tv, ok := cond.tags[tag]; ok {
                  if testTagVal(val, tv) {
                     found = true
                     break
                  }
               }
               for _, tagex := range cond.tagex {
                   if tagex.regex.MatchString(tag) {
                       if testTagVal(val, tagex.value) {
                         found = true
                         break
                       }
                   }
               }
           }
           if found == false {
              return false
           }
    }
    return true
}

// check tags contain features from a groups of whitelists
func containsValidTags(tags map[string]string, groups map[int][]*TagSelector) (map[string]string,  bool) {
    if hasTags(tags) {
        tags = trimTags(tags)
        for _, list := range groups {
            if matchTagsAgainstCompulsoryTagList(tags, list) {
               return tags, true
            }
        }
    }
    return tags, false
}


func xwayOnly(tags map[string]string, tag string, groups map[int][]*TagSelector) bool {
    delete(tags, tag) // remove examined property
    // check if target is interesting because of other tags
    for _, list := range groups {
        if matchTagsAgainstCompulsoryTagList(tags, list) {
            return false
        }
    }
    return true
}

// check if tags contain features which are useful for address translations
// also, group identially named segmented ways (highways, waterways)
func toDictionary(ID int64, mtype osmpbf.MemberType, tags map[string]string, dictionaryIds map[int64]bool,  context *context) bool {

    if hasTags(tags) {
        if htype, okh := tags["highway"]; okh {
            if (context.config.highways != nil) {
               // highway type filter given, validate against allowed values
               if _, validHWtype := context.config.highways[htype]; !validHWtype {
                  return false
               }
            }
	    var ok bool
	    var name string
            namefi, okfi := tags["name:fi"]
            namesv, oksv := tags["name:sv"];
            if namedef, ok2 := tags["name"]; ok2 {
                name = namedef
                ok = true
	    } else if okfi {
                name = namefi
                ok = true
            } else if oksv {
	        name = namesv
                ok = true
            }
            if ok {
	        hastransl := false
                cid := cacheId{ID, mtype}
                context.streets[name] = append(context.streets[name], cid)
                for k, v := range tags {
                  for namekey, _ := range context.config.names {
                    if strings.HasPrefix(k, namekey) {
		       	if  v != name {
			     context.translations[name] = append(context.translations[name], cid)
			     hastransl = true
			}
			if okfi && namefi != name && v != namefi {
			    context.translations[namefi] = append(context.translations[namefi], cid)
			    hastransl = true
                        }
                        if oksv && namesv != name && v != namesv {
			    context.translations[namesv] = append(context.translations[namesv], cid)
			    hastransl = true
                        }
                    }
                  }
                }
		if hastransl {
		    dictionaryIds[ID] = true
		    return true
		}
            }
        }
        if _, ok := tags["waterway"]; ok {
            if name, ok2 := tags["name"]; ok2 {
                cid := cacheId{ID, mtype}
                context.waterways[name] = append(context.waterways[name], cid)
            }
        }
    }
    return false
}

func translateAddress(tags map[string]string, location *Point, context *context) {
    var streetname, housenumber string
    var ok, address bool

    if streetname, ok = tags["addr:street"]; ok {
        if housenumber, ok = tags["addr:housenumber"]; ok {
           address = true
        }
    }
    if amenity, hasAmenity := tags["amenity"]; hasAmenity {
        if names, hasNames := amenityNames[amenity]; hasNames {
             for lang, name := range names {
                 var key string
                 if lang == "fi" {
                    key = "alt_name" // don't overrule potential "name" property
                 } else {
                    key = "name:" + lang
                 }
                 if _, hasLang := tags[key]; !hasLang {
                    // use unused name slot
                    tags[key] = name
                    context.amenitycount++
                 }
             }
        }
    }
    if !address {
       return
    }

    var tags2 map[string]string

    if translations, ok2 := context.translations[streetname]; ok2 {
        for _, cid := range translations {
            var wr *jsonWayRel
            var ok bool

            switch cid.Type {
              case osmpbf.WayType:
                wr, ok = cacheFetch(context.ways, cid.ID).(*jsonWayRel)

              case osmpbf.RelationType:
                wr, ok = context.formattedRelations[cid.ID]
            }

            if !ok {
               continue
            }
            if !insideBBox(location, &wr.BBoxMin, &wr.BBoxMax) {
               continue
            }
            tags2 = wr.Tags

            for k, v := range tags2 {
                if strings.HasPrefix(k, "name:") { // language version
                    postfix := strings.TrimPrefix(k, "name:")
                    k2 := "addr:street:" + postfix // eg addr:street:sv
                    if _, ok = tags[k2]; !ok { // not yet used
                        tags[k2] = v // new translation
			context.transcount++
			if postfix == "fi" {
			  context.fitranscount++
			}
                    }
                } else if streetname != v  { // check for alt names, including xxx_name:lang
                  for namekey, _ := range context.config.names {
                    if strings.HasPrefix(k, namekey) && !strings.Contains(v, housenumber)  {
                       // this is a trick to pass unsupported streetname versions to pelias model
                       // we mark alternative names as addr:street:<alt_name>, eg.addr:street:short_name
                       // osm importer tracks such names and indexes them properly
                       k2 := "addr:street:" + namekey
                       if _, ok = tags[k2]; !ok {
                          tags[k2] = v
                          context.transcount++
                       }
                    }
                  }
                }
            }
        }
    }
}

// merge way segments into one
func mergeSegments(context *context, src map[string][]cacheId, dst map[int64]*jsonWayRel) {

    for _, cids := range src {
       var current *jsonWayRel = nil
       i1 := 0
       i2 := len(cids) - 1
       // iterate cids array until all connected sections are processed
       // collect processed entries to the start of array. i1 points to first unfinished item
       for ; i1 <= i2; {
           var wr *jsonWayRel
           var ok bool
           added := false
           for i := i1; i <= i2; i++ {
             cid := cids[i]
             switch cid.Type {
                case osmpbf.WayType:
                    wr, ok = cacheFetch(context.ways, cid.ID).(*jsonWayRel)
                case osmpbf.RelationType:
                    wr, ok = context.formattedRelations[cid.ID]
                default:
                   ok = false
             }
             if ok {
                if current == nil {
                   current = wr
                   dst[cid.ID]=wr
                   i1++
                } else {
                   if BBoxIntersects(&wr.BBoxMin, &wr.BBoxMax, &current.BBoxMin, &current.BBoxMax) {
                      added = true
                      sumBBox(&wr.BBoxMin, &wr.BBoxMax, &current.BBoxMin, &current.BBoxMax)
                      // assign additional name tags
                      for k, v := range wr.Tags {
                          if strings.HasPrefix(k, "name:") {
                             current.Tags[k] = v
                          }
                      }
                      if i > i1 {
                         // move unfinished item from start to current array index
                         cids[i] = cids[i1]
                      }
                      i1++
                   }
                }
             }
           }
           if !added {
             if current != nil {
                current = nil
             } else {
               break // nothing to add any more
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
        if val == "yes" || val == "staircase" || val == "home" {
           return Point{Lat: node.Lat, Lon: node.Lon}, "entrance"
        }
    }
    return Point{}, ""
}
