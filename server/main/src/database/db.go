package database

import (
	"bufio"
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	//_ "github.com/mattn/go-sqlite3"
	_ "github.com/go-sql-driver/mysql"
	"github.com/mr-tron/base58/base58"
	"github.com/pkg/errors"
	"github.com/schollz/find3/server/main/src/models"
	"github.com/schollz/sqlite3dump"
	"github.com/schollz/stringsizer"
)

// Mysql config
var mysqlUser = "root"
var mysqlPW = "root"
var dbNamePrefix = "find3_"

// MakeTables creates two tables, a `keystore` table:
//
// 	KEY (TEXT)	VALUE (TEXT)
//
// and also a `sensors` table for the sensor data:
//
// 	TIMESTAMP (INTEGER)	DEVICE(TEXT) LOCATION(TEXT)
//
// the sensor table will dynamically create more columns as new types
// of sensor data are inserted. The LOCATION column is optional and
// only used for learning/classification.
func (d *Database) MakeTables() (err error) {
	sqlStmt := `create table keystore (key text not null primary key, value text);`
	_, err = d.db.Exec(sqlStmt)
	if err != nil {
		err = errors.Wrap(err, "MakeTables")
		logger.Log.Error(err)
		return
	}
	sqlStmt = `create index keystore_idx on keystore(key);`
	_, err = d.db.Exec(sqlStmt)
	if err != nil {
		err = errors.Wrap(err, "MakeTables")
		logger.Log.Error(err)
		return
	}
	sqlStmt = `create table sensors (timestamp integer not null primary key, deviceid text, locationid text, unique(timestamp));`
	_, err = d.db.Exec(sqlStmt)
	if err != nil {
		err = errors.Wrap(err, "MakeTables")
		logger.Log.Error(err)
		return
	}
	sqlStmt = `CREATE TABLE location_predictions (timestamp integer NOT NULL PRIMARY KEY, prediction TEXT, UNIQUE(timestamp));`
	_, err = d.db.Exec(sqlStmt)
	if err != nil {
		err = errors.Wrap(err, "MakeTables")
		logger.Log.Error(err)
		return
	}
	sqlStmt = `CREATE TABLE devices (id TEXT PRIMARY KEY, name TEXT);`
	_, err = d.db.Exec(sqlStmt)
	if err != nil {
		err = errors.Wrap(err, "MakeTables")
		logger.Log.Error(err)
		return
	}
	sqlStmt = `CREATE TABLE locations (id TEXT PRIMARY KEY, name TEXT);`
	_, err = d.db.Exec(sqlStmt)
	if err != nil {
		err = errors.Wrap(err, "MakeTables")
		logger.Log.Error(err)
		return
	}

	sqlStmt = `CREATE TABLE gps (id INTEGER PRIMARY KEY, timestamp INTEGER, mac TEXT, loc TEXT, lat REAL, lon REAL, alt REAL);`
	_, err = d.db.Exec(sqlStmt)
	if err != nil {
		err = errors.Wrap(err, "MakeTables")
		logger.Log.Error(err)
		return
	}

	sqlStmt = `create index devices_name on devices (name);`
	_, err = d.db.Exec(sqlStmt)
	if err != nil {
		err = errors.Wrap(err, "MakeTables")
		logger.Log.Error(err)
		return
	}

	sqlStmt = `CREATE INDEX sensors_devices ON sensors (deviceid);`
	_, err = d.db.Exec(sqlStmt)
	if err != nil {
		err = errors.Wrap(err, "MakeTables")
		logger.Log.Error(err)
		return
	}

	sensorDataSS, _ := stringsizer.New()
	err = d.Set("sensorDataStringSizer", sensorDataSS.Save())
	if err != nil {
		return
	}
	return
}

// Columns will list the columns
func (d *Database) Columns() (columns []string, err error) {
	rows, err := d.db.Query("SELECT * FROM sensors LIMIT 1")
	if err != nil {
		err = errors.Wrap(err, "Columns")
		return
	}
	columns, err = rows.Columns()
	rows.Close()
	if err != nil {
		err = errors.Wrap(err, "Columns")
		return
	}
	return
}

// Get will retrieve the value associated with a key.
func (d *Database) Get(key string, v interface{}) (err error) {
	stmt, err := d.db.Prepare("select value from keystore where id = ?")
	if err != nil {
		return errors.Wrap(err, "problem preparing SQL")
	}
	defer stmt.Close()
	var result string
	if err = stmt.QueryRow(key).Scan(&result); err != nil {
		return errors.Wrap(err, "problem getting key")
	}

	err = json.Unmarshal([]byte(result), &v)
	if err != nil {
		return
	}
	// logger.Log.Debugf("got %s from '%s'", string(result), key)
	return
}

// GetMany will retrieve values associated with a set of keys.
func (d *Database) GetMany(keyValues map[string]interface{}) (err error) {
	// SQL
	keys := make([]string, 0)
	for key := range keyValues {
		keys = append(keys, key)
	}
	sql := "select id,value from keystore where id IN ('" + strings.Join(keys, "','") + "')"
	stmt, err := d.db.Prepare(sql)
	if err != nil {
		return errors.Wrap(err, "problem preparing SQL")
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		return errors.Wrap(err, "problem executing SQL")
	}
	defer rows.Close()

	// Put SQL results into keyValues
	for rows.Next() {
		var id string
		var value string
		if err = rows.Scan(&id, &value); err != nil {
			return errors.Wrap(err, "problem scanning SQL rows")
		}
		if err = json.Unmarshal([]byte(value), keyValues[id]); err != nil {
			return errors.Wrap(err, "problem unmarshalling results")
		}
	}

	return
}

// Set will set a value in the database, when using it like a keystore.
func (d *Database) Set(key string, value interface{}) (err error) {
	var b []byte
	if b, err = json.Marshal(value); err != nil {
		return
	}
	valueStr := string(b)
	sql := "insert into keystore(id,value) values(?,?) on duplicate key update value=?"
	stmt, err := d.db.Prepare(sql)
	if err != nil {
		return errors.Wrap(err, "Set")
	}
	defer stmt.Close()

	if _, err = stmt.Exec(key, valueStr, valueStr); err != nil {
		return errors.Wrap(err, "Set")
	}

	// logger.Log.Debugf("set '%s' to '%s'", key, string(b))
	return
}

// Dump will output the string version of the database
func (d *Database) Dump() (dumped string, err error) {
	var b bytes.Buffer
	out := bufio.NewWriter(&b)
	err = sqlite3dump.Dump(d.name, out)
	if err != nil {
		return
	}
	out.Flush()
	dumped = string(b.Bytes())
	return
}

// GetAllFingerprints returns all the fingerprints
func (d *Database) GetAllFingerprints() (s []models.SensorData, err error) {
	return d.GetAllFromQuery("SELECT * FROM sensors ORDER BY timestamp")
}

// AddPrediction will insert or update a prediction in the database
func (d *Database) AddPrediction(timestamp int64, aidata []models.LocationPrediction) (err error) {
	// make sure we have a prediction
	if len(aidata) == 0 {
		err = errors.New("no predictions to add")
		return
	}

	// truncate to two digits
	for i := range aidata {
		aidata[i].Probability = float64(int64(float64(aidata[i].Probability)*100)) / 100
	}

	var b []byte
	if b, err = json.Marshal(aidata); err != nil {
		return err
	}
	stmt, err := d.db.Prepare("insert into location_predictions (timestamp,prediction) values (?,?) on duplicate key update prediction=?")
	if err != nil {
		return errors.Wrap(err, "stmt AddPrediction")
	}
	defer stmt.Close()

	if _, err = stmt.Exec(timestamp, string(b), string(b)); err != nil {
		return errors.Wrap(err, "exec AddPrediction")
	}

	return
}

// GetPrediction will retrieve models.LocationAnalysis associated with that timestamp
func (d *Database) GetPrediction(timestamp int64) (aidata []models.LocationPrediction, err error) {
	stmt, err := d.db.Prepare("SELECT prediction FROM location_predictions WHERE timestamp = ?")
	if err != nil {
		err = errors.Wrap(err, "problem preparing SQL")
		return
	}
	defer stmt.Close()
	var result string
	err = stmt.QueryRow(timestamp).Scan(&result)
	if err != nil {
		err = errors.Wrap(err, "problem getting key")
		return
	}

	err = json.Unmarshal([]byte(result), &aidata)
	if err != nil {
		return
	}
	// logger.Log.Debugf("got %s from '%s'", string(result), key)
	return
}

// AddSensor will insert a sensor data into the database
// TODO: AddSensor should be special case of AddSensors
func (d *Database) AddSensor(s models.SensorData) (err error) {
	startTime := time.Now()
	// determine the current table coluss
	oldColumns := make(map[string]struct{})
	columnList, err := d.Columns()
	if err != nil {
		return
	}
	for _, column := range columnList {
		oldColumns[column] = struct{}{}
	}

	// get string sizer
	var sensorDataStringSizerString string
	err = d.Get("sensorDataStringSizer", &sensorDataStringSizerString)
	if err != nil {
		return
	}
	sensorDataSS, err := stringsizer.New(sensorDataStringSizerString)
	if err != nil {
		return
	}
	previousCurrent := sensorDataSS.Current

	args := make([]interface{}, 4)
	args[0] = s.Timestamp
	args[1] = s.Device
	args[2] = s.Location
	args[3] = sensorDataSS.ShrinkMapToString(s.Sensors["bluetooth"])

	sqlStatement := "insert into sensors(timestamp,deviceid,locationid,bluetooth) values (?,?,?,?)"
	stmt, err := d.db.Prepare(sqlStatement)
	if err != nil {
		return errors.Wrap(err, "AddSensor, prepare "+sqlStatement)
	}
	defer stmt.Close()

	if _, err = stmt.Exec(args...); err != nil {
		return errors.Wrap(err, "AddSensor, execute")
	}

	// update the map key slimmer
	if previousCurrent != sensorDataSS.Current {
		if err = d.Set("sensorDataStringSizer", sensorDataSS.Save()); err != nil {
			return
		}
	}

	logger.Log.Debugf("[%s] inserted sensor data, %s", s.Family, time.Since(startTime))
	return

}

// GetSensorFromTime will return a sensor data for a given timestamp
func (d *Database) GetSensorFromTime(timestamp interface{}) (s models.SensorData, err error) {
	sensors, err := d.GetAllFromPreparedQuery("SELECT * FROM sensors WHERE timestamp = ?", timestamp)
	if err != nil {
		err = errors.Wrap(err, "GetSensorFromTime")
	} else {
		s = sensors[0]
	}
	return
}

// GetLastSensorTimestamp gets will retrieve the value associated with a key.
func (d *Database) GetLastSensorTimestamp() (timestamp int64, err error) {
	stmt, err := d.db.Prepare("SELECT timestamp FROM sensors ORDER BY timestamp DESC LIMIT 1")
	if err != nil {
		err = errors.Wrap(err, "problem preparing SQL")
		return
	}
	defer stmt.Close()
	err = stmt.QueryRow().Scan(&timestamp)
	if err != nil {
		err = errors.Wrap(err, "problem getting key")
	}
	return
}

// TotalLearnedCount gets will retrieve the value associated with a key.
func (d *Database) TotalLearnedCount() (count int64, err error) {
	stmt, err := d.db.Prepare("SELECT count(timestamp) FROM sensors WHERE locationid != ''")
	if err != nil {
		err = errors.Wrap(err, "problem preparing SQL")
		return
	}
	defer stmt.Close()
	err = stmt.QueryRow().Scan(&count)
	if err != nil {
		err = errors.Wrap(err, "problem getting key")
	}
	return
}

// GetSensorFromGreaterTime will return a sensor data for a given timeframe
func (d *Database) GetSensorFromGreaterTime(timeBlockInMilliseconds int64) (sensors []models.SensorData, err error) {
	latestTime, err := d.GetLastSensorTimestamp()
	if err != nil {
		return
	}
	minimumTimestamp := latestTime - timeBlockInMilliseconds
	logger.Log.Debugf("using minimum timestamp of %d", minimumTimestamp)
	sensors, err = d.GetAllFromPreparedQuery("SELECT * FROM sensors WHERE timestamp > ? GROUP BY deviceid ORDER BY timestamp DESC", minimumTimestamp)
	return
}

func (d *Database) GetDeviceFirstTimeFromDevices(devices []string) (firstTime map[string]time.Time, err error) {
	firstTime = make(map[string]time.Time)
	query := fmt.Sprintf("select d.name as n, max(s.timestamp) as t from sensors as s where devices.name IN ('%s') left join devices as d on s.deviceid = d.id group by d.id", strings.Join(devices, "','"))

	stmt, err := d.db.Prepare(query)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var ts int64
		err = rows.Scan(&name, &ts)
		if err != nil {
			err = errors.Wrap(err, "scanning")
			return
		}
		// if _, ok := firstTime[name]; !ok {
		firstTime[name] = time.Unix(0, ts*1000000).UTC()
		// }
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "rows")
	}
	return
}

func (d *Database) GetDeviceFirstTime() (firstTime map[string]time.Time, err error) {

	firstTime = make(map[string]time.Time)
	query := "select n,t from (select devices.name as n,sensors.timestamp as t from sensors inner join devices on sensors.deviceid=devices.id order by timestamp desc) group by n"
	// query := "select devices.name,sensors.timestamp from sensors inner join devices on sensors.deviceid=devices.id order by timestamp desc"
	stmt, err := d.db.Prepare(query)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var ts int64
		err = rows.Scan(&name, &ts)
		if err != nil {
			err = errors.Wrap(err, "scanning")
			return
		}
		// if _, ok := firstTime[name]; !ok {
		firstTime[name] = time.Unix(0, ts*1000000).UTC()
		// }
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "rows")
	}
	return
}

func (d *Database) GetDeviceCountsFromDevices(devices []string) (counts map[string]int, err error) {
	counts = make(map[string]int)
	query := fmt.Sprintf("select devices.name,count(sensors.timestamp) as num from sensors inner join devices on sensors.deviceid=devices.id WHERE devices.name in ('%s') group by sensors.deviceid", strings.Join(devices, "','"))
	stmt, err := d.db.Prepare(query)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var count int
		err = rows.Scan(&name, &count)
		if err != nil {
			err = errors.Wrap(err, "scanning")
			return
		}
		counts[name] = count
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "rows")
	}
	return
}

func (d *Database) GetDeviceCounts() (counts map[string]int, err error) {
	counts = make(map[string]int)
	query := "select devices.name,count(sensors.timestamp) as num from sensors inner join devices on sensors.deviceid=devices.id group by sensors.deviceid"
	stmt, err := d.db.Prepare(query)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var count int
		err = rows.Scan(&name, &count)
		if err != nil {
			err = errors.Wrap(err, "scanning")
			return
		}
		counts[name] = count
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "rows")
	}
	return
}

func (d *Database) GetLocationCounts() (counts map[string]int, err error) {
	counts = make(map[string]int)
	query := "SELECT locationid, count(timestamp) as num from sensors group by locationid"
	stmt, err := d.db.Prepare(query)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		var count int
		err = rows.Scan(&name, &count)
		if err != nil {
			err = errors.Wrap(err, "scanning")
			return
		}
		counts[name] = count
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "rows")
	}
	return
}

// GetAllForClassification will return a sensor data for classifying
func (d *Database) GetAllForClassification() (s []models.SensorData, err error) {
	return d.GetAllFromQuery("SELECT timestamp, deviceid, locationid, bluetooth FROM sensors WHERE sensors.locationid !='' AND status = 'active' ORDER BY timestamp")
}

// GetAllNotForClassification will return a sensor data for classifying
func (d *Database) GetAllNotForClassification() (s []models.SensorData, err error) {
	return d.GetAllFromQuery("SELECT * FROM sensors WHERE sensors.locationid =='' ORDER BY timestamp")
}

// GetLatest will return a sensor data for classifying
func (d *Database) GetLatest(device string) (s models.SensorData, err error) {
	var sensors []models.SensorData
	sensors, err = d.GetAllFromPreparedQuery("SELECT * FROM sensors WHERE deviceid=? ORDER BY timestamp DESC LIMIT 1", strings.TrimSpace(device))
	if err != nil {
		return
	}
	if len(sensors) > 0 {
		s = sensors[0]
	} else {
		err = errors.New("no rows found")
	}
	return
}

func (d *Database) GetKeys(keylike string) (keys []string, err error) {
	query := "SELECT key FROM keystore WHERE key LIKE ?"
	stmt, err := d.db.Prepare(query)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query(keylike)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer rows.Close()

	keys = []string{}
	for rows.Next() {
		var key string
		err = rows.Scan(&key)
		if err != nil {
			err = errors.Wrap(err, "scanning")
			return
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "rows")
	}
	return
}

func (d *Database) GetDevices() (devices []string, err error) {
	query := "SELECT devicename FROM (SELECT devices.name as devicename,COUNT(devices.name) as counts FROM sensors INNER JOIN devices ON sensors.deviceid = devices.id GROUP by devices.name) ORDER BY counts DESC"
	stmt, err := d.db.Prepare(query)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer rows.Close()

	devices = []string{}
	for rows.Next() {
		var name string
		err = rows.Scan(&name)
		if err != nil {
			err = errors.Wrap(err, "scanning")
			return
		}
		devices = append(devices, name)
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, fmt.Sprintf("problem scanning rows, only got %d devices", len(devices)))
	}
	return
}

func (d *Database) GetIDToName(table string) (idToName map[string]string, err error) {
	idToName = make(map[string]string)
	query := "SELECT id,name FROM " + table
	stmt, err := d.db.Prepare(query)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var name, id string
		err = rows.Scan(&id, &name)
		if err != nil {
			err = errors.Wrap(err, "scanning")
			return
		}
		idToName[id] = name
	}
	err = rows.Err()
	if err != nil {
		err = errors.Wrap(err, "rows")
	}
	return
}

func GetFamilies() (families []string) {
	files, err := ioutil.ReadDir(DataFolder)
	if err != nil {
		log.Fatal(err)
	}

	families = make([]string, len(files))
	i := 0
	for _, f := range files {
		if !strings.Contains(f.Name(), ".sqlite3.db") {
			continue
		}
		b, err := base58.Decode(strings.TrimSuffix(f.Name(), ".sqlite3.db"))
		if err != nil {
			continue
		}
		families[i] = string(b)
		i++
	}
	if i > 0 {
		families = families[:i]
	} else {
		families = []string{}
	}
	return
}

func (d *Database) DeleteLocation(locationName string) (err error) {
	id, err := d.GetID("locations", locationName)
	if err != nil {
		return
	}
	stmt, err := d.db.Prepare("DELETE FROM sensors WHERE locationid = ?")
	if err != nil {
		err = errors.Wrap(err, "problem preparing SQL")
		return

	}
	defer stmt.Close()
	_, err = stmt.Exec(id)
	return
}

// GetID will get the ID of an element in a table (devices/locations) and return an error if it doesn't exist
func (d *Database) GetID(table string, name string) (id string, err error) {
	// first check to see if it has already been added
	stmt, err := d.db.Prepare("SELECT id FROM " + table + " WHERE name = ?")
	defer stmt.Close()
	if err != nil {
		err = errors.Wrap(err, "problem preparing SQL")
		return
	}
	err = stmt.QueryRow(name).Scan(&id)
	return
}

// GetName will get the name of an element in a table (devices/locations) and return an error if it doesn't exist
func (d *Database) GetName(table string, id string) (name string, err error) {
	// first check to see if it has already been added
	stmt, err := d.db.Prepare("SELECT name FROM " + table + " WHERE id = ?")
	defer stmt.Close()
	if err != nil {
		err = errors.Wrap(err, "problem preparing SQL")
		return
	}
	err = stmt.QueryRow(id).Scan(&name)
	return
}

// Open will open the database for transactions by first aquiring a filelock.
func Open(family string, readOnly ...bool) (d *Database, err error) {
	d = new(Database)
	d.family = strings.ToLower(strings.TrimSpace(family))
	d.name = "find3_" + d.family

	// obtain a lock on the database
	// logger.Log.Debugf("getting filelock on %s", d.name+".lock")
	/*
		for {
			var ok bool
			// use Rlock instead of lock if readonly
			databaseLock.Lock()
			if _, ok = databaseLock.Locked[d.name]; !ok {
				databaseLock.Locked[d.name] = true
			}
			databaseLock.Unlock()
			if !ok {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	*/
	// logger.Log.Debugf("got filelock")

	// TODO: check if it is a new database

	// open database
	if d.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@/%s%s", mysqlUser, mysqlPW, dbNamePrefix, d.family)); err == nil {
		logger.Log.Debug("opened mysql database")
	}

	// create new database tables if needed
	/*
		if newDatabase {
			err = d.MakeTables()
			if err != nil {
				return
			}
			logger.Log.Debug("made tables")
		}
	*/

	return
}

func (d *Database) Debug(debugMode bool) {
	if debugMode {
		logger.SetLevel("debug")
	} else {
		logger.SetLevel("info")
	}
}

// Close will close the database connection and remove the filelock.
func (d *Database) Close() (err error) {
	if d.isClosed {
		return
	}
	// close database
	err2 := d.db.Close()
	if err2 != nil {
		err = err2
		logger.Log.Error(err)
	}

	// close filelock
	// logger.Log.Debug("closing lock")
	databaseLock.Lock()
	delete(databaseLock.Locked, d.name)
	databaseLock.Unlock()
	d.isClosed = true
	return
}

func (d *Database) GetAllFromQuery(query string) (s []models.SensorData, err error) {
	// logger.Log.Debug(query)
	rows, err := d.db.Query(query)
	if err != nil {
		err = errors.Wrap(err, "GetAllFromQuery")
		return
	}
	defer rows.Close()

	// parse rows
	s, err = d.getRows(rows)
	if err != nil {
		err = errors.Wrap(err, query)
	}
	return
}

// GetAllFromPreparedQuery
func (d *Database) GetAllFromPreparedQuery(query string, args ...interface{}) (s []models.SensorData, err error) {
	// prepare statement
	// startQuery := time.Now()
	stmt, err := d.db.Prepare(query)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	defer stmt.Close()
	rows, err := stmt.Query(args...)
	if err != nil {
		err = errors.Wrap(err, query)
		return
	}
	// logger.Log.Debugf("%s: %s", query, time.Since(startQuery))
	// startQuery = time.Now()
	defer rows.Close()
	s, err = d.getRows(rows)
	if err != nil {
		err = errors.Wrap(err, query)
	}
	// logger.Log.Debugf("getRows %s: %s", query, time.Since(startQuery))
	return
}

func (d *Database) getRows(rows *sql.Rows) (sensorData []models.SensorData, err error) {
	// get the string sizer for the sensor data
	logger.Log.Debug("getting sensorstringsizer")
	var sensorDataStringSizerString string
	if err = d.Get("sensorDataStringSizer", &sensorDataStringSizerString); err != nil {
		return
	}
	sensorDataSS, err := stringsizer.New(sensorDataStringSizerString)
	if err != nil {
		return
	}

	// loop through rows of sql result
	var (
		timestamp  int64
		deviceid   string
		locationid string
		bluetooth  string
	)
	sensorData = []models.SensorData{}
	for rows.Next() {
		if err = rows.Scan(&timestamp, &deviceid, &locationid, &bluetooth); err != nil {
			err = errors.Wrap(err, "getRows")
			return
		}

		s := models.SensorData{
			Timestamp: timestamp,
			Family:    d.family,
			Device:    deviceid,
			Location:  locationid,
			Sensors:   make(map[string]map[string]interface{}),
		}
		if s.Sensors["bluetooth"], err = sensorDataSS.ExpandMapFromString(bluetooth); err != nil {
			return
		}
		sensorData = append(sensorData, s)
	}

	if err = rows.Err(); err != nil {
		err = errors.Wrap(err, "getRows")
	}

	return
}

// SetGPS will set a GPS value in the GPS database
func (d *Database) SetGPS(p models.SensorData) (err error) {
	tx, err := d.db.Begin()
	if err != nil {
		return errors.Wrap(err, "SetGPS")
	}
	stmt, err := tx.Prepare("insert or replace into gps(timestamp ,mac, loc, lat, lon, alt) values (?, ?, ?, ?, ?,?)")
	if err != nil {
		return errors.Wrap(err, "SetGPS")
	}
	defer stmt.Close()

	for sensorType := range p.Sensors {
		for mac := range p.Sensors[sensorType] {
			_, err = stmt.Exec(p.Timestamp, sensorType+"-"+mac, p.Location, p.GPS.Latitude, p.GPS.Longitude, p.GPS.Altitude)
			if err != nil {
				return errors.Wrap(err, "SetGPS")
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		return errors.Wrap(err, "SetGPS")
	}
	return
}
