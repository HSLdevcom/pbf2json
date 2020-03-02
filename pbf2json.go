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
// import "github.com/davecgh/go-spew/spew"

const streetHitDistance = 0.005 // in wgs coords, some hundreds of meters

type Point struct {
    Lat  float64 `json:"lat"`
    Lon  float64 `json:"lon"`
}

type TagValue struct {
     values map[string]bool
     regex string
     any bool
}

type TagSelector struct {
    tags map[string]*TagValue
    regex map[string]*TagValue
}

type settings struct {
    PbfPath    string
    LevedbPath string
    Tags map[int][]*TagSelector
    BatchSize  int
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

    config *settings
    transcount int64
}

func getSettings() settings {

    // command line flags
    leveldbPath := flag.String("leveldb", "/tmp", "path to leveldb directory")

    tagList := flag.String("tags", "", "comma-separated list of valid tags, group AND conditions with a §")
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
    groups := make(map[int][]*TagSelector)
    for i, group := range strings.Split(*tagList, ",") { // top level alternatives
        for _, conditions := range strings.Split(group, "§") { // AND combined conditions for a single alternative
            condition := &TagSelector{make(map[string]*TagValue), make(map[string]*TagValue)}
            for _, tag := range strings.Split(conditions, "!") { // tag alternatives, combined with OR
                tv := TagValue{make(map[string]bool), "", false}
                pair := strings.Split(tag, "~") // tag name + optional value(s)
                if len(pair) > 1 {
                   valueDef := pair[1];
                   p1 := strings.LastIndex(valueDef, "#") // regex?
                   if p1 >= 0 {
                      tv.regex = valueDef[p1+1:]
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
                    condition.regex[tname[pos+1:]] = &tv;
                } else {
                    condition.tags[tname] = &tv;
                }
            }
            groups[i] = append(groups[i], condition)
        }
    }
    // spew.Dump(groups)

    return settings{args[0], *leveldbPath, groups, *batchSize}
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

    context.streets = make(map[string][]cacheId) // all streets collected here for merge process
    context.mergedStreets = make(map[int64]*jsonWayRel)
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

    // merge street segments
    mergeStreets(&context)

    // output items that match tag selection
    outputValidEntries(&context)

    // fmt.Printf("Translated address point count: %d\n", context.transcount)

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
                toStreetDictionary(v.ID, osmpbf.RelationType, tags, context.dictionaryRelations, context)
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
                toDict := toStreetDictionary(v.ID, osmpbf.WayType, tags, context.dictionaryWays, context)
                if ok || toDict || context.wayRef[v.ID] == true {
                    for _, each := range v.NodeIDs {
                        context.nodeRef[each] = true
                    }
                }
            }
        }
    }
    // fmt.Printf("Dictionary size %d\n", len(context.translations))
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
        if _, ok := way.Tags["highway"]; !ok { // don't output street segments
            translateAddress(way.Tags, &way.Centroid, context)
            printJson(way)
        }
    }
    for id, _ := range context.validRelations {
        relation := context.formattedRelations[id]
        if _, ok := relation.Tags["highway"]; !ok {
           translateAddress(relation.Tags, &relation.Centroid, context)
           printJson(relation)
        }
    }
    for _, street := range context.mergedStreets {
        if _, ok := containsValidTags(street.Tags, context.config.Tags); ok {
           printJson(street)
        }
    }
}


func printJson(v interface{}) {
    json, _ := json.Marshal(v)
    fmt.Println(string(json))
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

func entranceLookup(db *leveldb.DB, way *osmpbf.Way) (location Point, entranceType string) {
     var foundLocation Point
     eType := ""

     for _, each := range way.NodeIDs {
        node, ok := cacheFetch(db, each).(*jsonNode)
        if !ok {
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
    if isBuilding {
        centroid, centroidType = entranceLookup(context.nodes, way)
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
                    if centroidType == "" || cType == "mainEntrance" {
                        centroid = val
                        centroidType = cType
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
                    if centroidType == "" || cType == "mainEntrance" {
                        centroid = way.Centroid
                        centroidType = cType
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
                return nil
            }

            if cType, ok := relation.Tags["_centroidType"]; ok && cType != "average" {
                if centroidType == "" || cType == "mainEntrance" {
                    centroid = relation.Centroid
                    centroidType = cType
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
     if tv.regex != "" {
           if match, _ := regexp.MatchString(tv.regex, val); match {
               return true
           }
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
               for regex, tv := range cond.regex {
                   if match, _ := regexp.MatchString(regex, tag); match {
                       if testTagVal(val, tv) {
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

// check if tags contain features which are useful for address translations
// also, group identially named streets
func toStreetDictionary(ID int64, mtype osmpbf.MemberType, tags map[string]string, dictionaryIds map[int64]bool,  context *context) bool {

    if hasTags(tags) {
        if _, ok := tags["highway"]; ok {
            if name, ok2 := tags["name"]; ok2 {
                cid := cacheId{ID, mtype}
                context.streets[name] = append(context.streets[name], cid)
                for k, v := range tags {
                    if strings.Contains(k, "name:") && v != name {
                        dictionaryIds[ID] = true
                        context.translations[name] = append(context.translations[name], cid)
                        return true
                    }
                }
            }
        }
    }
    return false
}

func translateAddress(tags map[string]string, location *Point, context *context) {
    var streetname, housenumber string
    var ok bool
    if streetname, ok = tags["addr:street"]; !ok {
       return
    }
    if housenumber, ok = tags["addr:housenumber"]; !ok {
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
                if strings.HasPrefix(k, "name:") && streetname != v  {
                    if _, ok = tags[k]; false && !ok { // name:lang entry not yet in use
                       tags[k] = housenumber + " " + v // Hooray! Translated!
                    } else {
                        postfix := strings.TrimPrefix(k, "name:")
                        k2 := "addr:street:" + postfix // eg addr:street:sv
                        if _, ok = tags[k2]; !ok { // not yet used
                            tags[k2] = v
                            context.transcount += 1
                        }
                    }
                }
            }
        }
    }
}


// merge street segments into one
func mergeStreets(context *context) {

    for _, cids := range context.streets {
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
                   context.mergedStreets[cid.ID]=wr
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
        if val == "yes" {
           return Point{Lat: node.Lat, Lon: node.Lon}, "entrance"
        }
    }
    return Point{}, ""
}
