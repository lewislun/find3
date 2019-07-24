package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/gzip"
	"github.com/gin-gonic/gin"
	"github.com/pkg/errors"
	"github.com/schollz/find3/server/main/src/api"
	"github.com/schollz/find3/server/main/src/database"
	"github.com/schollz/find3/server/main/src/models"
	"github.com/schollz/find3/server/main/src/mqtt"
)

// Port defines the public port
var Port = "8003"
var UseSSL = false
var UseMQTT = false
var MinimumPassive = -1

// Database object
var db *database.Database

// Run will start the server listening on the specified port
func Run(debugMode bool) (err error) {
	defer logger.Log.Flush()

	if db, err = database.Open("default"); err != nil {
		logger.Log.Error("cannot open db, stopping...")
		return
	}
	defer db.Close()

	if UseMQTT {
		// setup MQTT
		err = mqtt.Setup(db)
		if err != nil {
			logger.Log.Warn(err)
		}
		logger.Log.Debug("setup mqtt")
	}

	// setup gin server
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(middleWareHandler(), gin.Recovery(), gzip.Gzip(gzip.DefaultCompression))
	// dashboard (disabled)
	/*
		r.LoadHTMLGlob("templates/*")
		r.Static("/static", "./static")
		r.HEAD("/", func(c *gin.Context) { // handler for the uptime robot
			c.String(http.StatusOK, "OK")
		})
		r.GET("/", func(c *gin.Context) { // handler for the uptime robot
			c.HTML(http.StatusOK, "login.tmpl", gin.H{
				"Message": "",
			})
		})
		r.POST("/", func(c *gin.Context) {
			family := strings.ToLower(c.PostForm("inputFamily"))
			c.Redirect(http.StatusMovedPermanently, "/view/dashboard/"+family)

			//c.HTML(http.StatusOK, "login.tmpl", gin.H{
			//	"Message": template.HTML(fmt.Sprintf(`Family '%s' does not exist. Follow <a href="https://www.internalpositioning.com/doc/tracking_your_phone.md" target="_blank">these instructions</a> to get started.`, family)),
			//})
		})
		r.GET("/view/analysis/:family", func(c *gin.Context) {
			family := strings.ToLower(c.Param("family"))
			locationList, err := db.GetLocations()
			if err != nil {
				logger.Log.Warn("could not get locations")
				c.String(200, err.Error())
				return
			}
			c.HTML(http.StatusOK, "analysis.tmpl", gin.H{
				"LocationAnalysis": true,
				"Family":           family,
				"Locations":        locationList,
				"FamilyJS":         template.JS(family),
			})
		})
		r.GET("/view/location_analysis/:family/:location", func(c *gin.Context) {
			family := strings.ToLower(c.Param("family"))
			img, err := api.GetImage(family, c.Param("location"))
			if err != nil {
				c.String(http.StatusBadRequest, fmt.Sprintf("unable to locate image for '%s' for '%s'", c.Param("location"), family))
			} else {
				c.Data(200, "image/png", img)
			}
		})
		r.GET("/view/dashboard/:family", func(c *gin.Context) {
			type LocEff struct {
				Name           string
				Total          int64
				PercentCorrect int64
			}
			type Efficacy struct {
				AccuracyBreakdown   []LocEff
				LastCalibrationTime time.Time
				TotalCount          int64
				PercentCorrect      int64
			}
			type DeviceTable struct {
				ID           string
				Name         string
				LastLocation string
				LastSeen     time.Time
				Probability  int64
				ActiveTime   int64
			}

			family := strings.ToLower(c.Param("family"))
			err := func(family string) (err error) {
				startTime := time.Now()
				var errorMessage string
				var efficacy Efficacy

				minutesAgoInt := 60
				millisecondsAgo := int64(minutesAgoInt * 60 * 1000)
				sensors, err := db.GetSensorFromGreaterTime(millisecondsAgo)
				logger.Log.Debugf("[%s] got sensor from greater time %s", family, time.Since(startTime))
				devicesToCheckMap := make(map[string]struct{})
				for _, sensor := range sensors {
					devicesToCheckMap[sensor.Device] = struct{}{}
				}
				// get list of devices I care about
				devicesToCheck := make([]string, len(devicesToCheckMap))
				i := 0
				for device := range devicesToCheckMap {
					devicesToCheck[i] = device
					i++
				}
				logger.Log.Debugf("[%s] found %d devices to check", family, len(devicesToCheck))

				logger.Log.Debugf("[%s] getting device counts", family)
				deviceCounts, err := db.GetDeviceCountsFromDevices(devicesToCheck)
				if err != nil {
					err = errors.Wrap(err, "could not get devices")
					return
				}

				deviceList := make([]string, len(deviceCounts))
				i = 0
				for device := range deviceCounts {
					if deviceCounts[device] > 2 {
						deviceList[i] = device
						i++
					}
				}
				deviceList = deviceList[:i]
				jsonDeviceList, _ := json.Marshal(deviceList)
				logger.Log.Debugf("found %d devices", len(deviceList))

				logger.Log.Debugf("[%s] getting locations", family)
				locationList, err := db.GetLocations()
				if err != nil {
					logger.Log.Warn("could not get locations")
				}
				jsonLocationList, _ := json.Marshal(locationList)
				logger.Log.Debugf("found %d locations", len(locationList))

				logger.Log.Debugf("[%s] total learned count", family)
				efficacy.TotalCount, err = db.TotalLearnedCount()
				if err != nil {
					logger.Log.Warn("could not get TotalLearnedCount")
				}

				var percentFloat64 float64
				var confusionMetrics map[string]map[string]models.BinaryStats
				var accuracyBreakdown map[string]float64

				keyValues := make(map[string]interface{})
				keyValues["PercentCorrect"] = &percentFloat64
				keyValues["LastCalibrationTime"] = &efficacy.LastCalibrationTime
				keyValues["AccuracyBreakdown"] = &accuracyBreakdown
				keyValues["AlgorithmEfficacy"] = &confusionMetrics
				if err := db.GetMany(keyValues); err != nil {
					err = errors.Wrap(err, "could not get info")
				}
				efficacy.PercentCorrect = int64(100 * percentFloat64)

				logger.Log.Debugf("[%s] getting location count", family)
				locationCounts, err := db.GetLocationCounts()
				if err != nil {
					logger.Log.Warn("could not get location counts")
				}
				logger.Log.Debugf("[%s] locations: %+v", family, locationCounts)

				efficacy.AccuracyBreakdown = make([]LocEff, len(accuracyBreakdown))
				i = 0
				for key := range accuracyBreakdown {
					l := LocEff{Name: strings.Title(key)}
					l.PercentCorrect = int64(100 * accuracyBreakdown[key])
					l.Total = int64(locationCounts[key])
					efficacy.AccuracyBreakdown[i] = l
					i++
				}
				var rollingData models.ReverseRollingData
				errRolling := db.Get("ReverseRollingData", &rollingData)
				passiveTable := []DeviceTable{}
				scannerList := []string{}
				if errRolling == nil {
					passiveTable = make([]DeviceTable, len(rollingData.DeviceLocation))
					i := 0
					for device := range rollingData.DeviceLocation {
						s, errOpen := db.GetLatest(device)
						if errOpen != nil {
							continue
						}
						passiveTable[i].Name = device
						passiveTable[i].LastLocation = rollingData.DeviceLocation[device]
						passiveTable[i].LastSeen = time.Unix(0, s.Timestamp*1000000).UTC()
						i++
					}
					sensors, errGet := db.GetSensorFromGreaterTime(60000 * 15)
					if errGet == nil {
						allScanners := make(map[string]struct{})
						for _, s := range sensors {
							for sensorType := range s.Sensors {
								for scanner := range s.Sensors[sensorType] {
									allScanners[scanner] = struct{}{}
								}
							}
						}
						scannerList = make([]string, len(allScanners))
						i = 0
						for scanner := range allScanners {
							scannerList[i] = scanner
							i++
						}
					}
				}

				logger.Log.Debugf("[%s] getting by_locations for %d devices", family, len(deviceCounts))
				// logger.Log.Debug(deviceCounts)
				byLocations, err := api.GetByLocation(family, 15, false, 3, 0, 0, deviceCounts, db)
				if err != nil {
					logger.Log.Warn(err)
				}

				logger.Log.Debugf("[%s] creating device table", family)
				table := []DeviceTable{}
				for _, byLocation := range byLocations {
					for _, device := range byLocation.Devices {
						table = append(table, DeviceTable{
							ID:           utils.Hash(device.Device),
							Name:         device.Device,
							LastLocation: byLocation.Location,
							LastSeen:     device.Timestamp,
							Probability:  int64(device.Probability * 100),
							ActiveTime:   int64(device.ActiveMins),
						})
					}
				}

				if err != nil {
					errorMessage = err.Error()
				} else if percentFloat64 == 0 {
					errorMessage = "No learning data available, see the documentation for how to get started with learning. "
				}
				if efficacy.LastCalibrationTime.IsZero() {
					errorMessage += "You need to calibrate, press the calibration button."
				}

				c.HTML(http.StatusOK, "dashboard.tmpl", gin.H{
					"Dashboard":      true,
					"Family":         family,
					"FamilyJS":       template.JS(family),
					"Efficacy":       efficacy,
					"Devices":        table,
					"ErrorMessage":   errorMessage,
					"PassiveDevices": passiveTable,
					"DeviceList":     template.JS(jsonDeviceList),
					"LocationList":   template.JS(jsonLocationList),
					"Scanners":       scannerList,
					"PercentCorrect": percentFloat64,
					"UseMQTT":        UseMQTT,
					"MQTTServer":     os.Getenv("MQTT_EXTERNAL"),
					"MQTTPort":       os.Getenv("MQTT_PORT"),
				})
				err = nil
				logger.Log.Debugf("[%s] rendered dashboard in %s", family, time.Since(startTime))
				return
			}(family)
			if err != nil {
				logger.Log.Warn(err)
				c.HTML(http.StatusOK, "dashboard.tmpl", gin.H{
					"Family":       family,
					"FamilyJS":     template.JS(family),
					"ErrorMessage": err.Error(),
					"Efficacy":     Efficacy{},
				})
			}
		})
	*/
	//r.OPTIONS("/api/v1/settings/passive", func(c *gin.Context) { c.String(200, "OK") })
	//r.POST("/api/v1/settings/passive", handlerReverseSettings)
	//r.GET("/ws", wshandler) // handler for the web sockets (see websockets.go)
	/*
		if UseMQTT {
			r.GET("/api/v1/mqtt/:family", handlerMQTT) // handler for setting MQTT
		}
	*/
	//r.POST("/passive", handlerReverse)       // typical data handler

	r.OPTIONS("/efficacy", func(c *gin.Context) { c.String(200, "OK") })
	r.GET("/efficacy", handlerEfficacy)
	r.GET("/now", handlerNow)
	r.POST("/locate", handlerLocate)

	if debugMode {
		r.OPTIONS("/calibrate", func(c *gin.Context) { c.String(200, "OK") })
		r.GET("/calibrate", handlerCalibrate)
		r.POST("/learn", handlerLearn)

		logger.Log.Infof("Debug Mode on. Learning and Calibration APIs enabled.")
	}
	logger.Log.Infof("Running on 0.0.0.0:%s", Port)

	err = r.Run(":" + Port) // listen and serve
	return
}

func handlerLocate(c *gin.Context) {
	analysis, err := func(c *gin.Context) (analysis models.LocationAnalysis, err error) {

		// get data
		var s models.SensorData
		if err = c.BindJSON(&s); err != nil {
			err = errors.Wrap(err, "problem binding data")
			return
		}

		// analyze data
		if analysis, err = api.AnalyzeSensorData(s, db); err != nil {
			return
		}
		// TODO: save data in db

		return
	}(c)

	if err != nil {
		logger.Log.Errorf("problem locating: %s", err.Error())
		c.JSON(http.StatusOK, gin.H{"message": err.Error(), "success": false})
	} else {
		c.JSON(http.StatusOK, gin.H{"guesses": analysis.Guesses, "success": true})
	}
}

func handlerEfficacy(c *gin.Context) {
	type Efficacy struct {
		AccuracyBreakdown   map[string]float64                       `json:"accuracy_breakdown"`
		ConfusionMetrics    map[string]map[string]models.BinaryStats `json:"confusion_metrics"`
		LastCalibrationTime time.Time                                `json:"last_calibration_time"`
	}

	efficacy, err := func(c *gin.Context) (efficacy Efficacy, err error) {
		keyValues := make(map[string]interface{})
		keyValues["LastCalibrationTime"] = &efficacy.LastCalibrationTime
		keyValues["AccuracyBreakdown"] = &efficacy.AccuracyBreakdown
		keyValues["AlgorithmEfficacy"] = &efficacy.ConfusionMetrics
		if err := db.GetMany(keyValues); err != nil {
			err = errors.Wrap(err, "could not get efficacy info")
		}
		return
	}(c)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"message": err.Error(), "success": err == nil})
	} else {
		c.JSON(http.StatusOK, gin.H{"message": "got stats", "success": err == nil, "efficacy": efficacy})
	}
}

func handlerCalibrate(c *gin.Context) {
	err := api.Calibrate("default", db, true)
	message := "calibrated data"
	if err != nil {
		message = err.Error()
	}
	c.JSON(http.StatusOK, gin.H{"message": message, "success": err == nil})
}

func handlerMQTT(c *gin.Context) {
	message, err := func(c *gin.Context) (message string, err error) {
		family := strings.ToLower(strings.TrimSpace(c.Param("family")))
		if family == "" {
			err = errors.New("invalid family")
			return
		}
		passphrase, err := mqtt.AddFamily(family)
		if err != nil {
			return
		}
		message = fmt.Sprintf("Added '%s' for mqtt. Your passphrase is '%s'", family, passphrase)
		return
	}(c)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"message": err.Error(), "success": err == nil})
	} else {
		c.JSON(http.StatusOK, gin.H{"message": message, "success": err == nil})
	}
	return
}

func sendOutLocation(family, device string) (s models.SensorData, analysis models.LocationAnalysis, err error) {
	s, err = db.GetLatest(device)
	if err != nil {
		return
	}
	analysis, err = sendOutData(s)
	if err != nil {
		return
	}
	analysis, err = api.AnalyzeSensorData(s, db)
	if err != nil {
		err = api.Calibrate(family, db, true)
		if err != nil {
			logger.Log.Warn(err)
			return
		}
	}
	return
}

func handlerNow(c *gin.Context) {
	c.String(200, strconv.Itoa(int(time.Now().UTC().UnixNano()/int64(time.Millisecond))))
}

func handlerLearn(c *gin.Context) {
	message, err := func(c *gin.Context) (message string, err error) {
		//justSave := c.DefaultQuery("justsave", "0") == "1"
		var s models.SensorData
		if err = c.BindJSON(&s); err != nil {
			message = s.Family
			err = errors.Wrap(err, "problem binding data")
			return
		}

		// process data
		if err = processSensorData(s, true); err != nil {
			message = s.Family
			return
		}

		// success
		message = "inserted data"
		logger.Log.Debugf("[%s] /data %+v", s.Family, s)
		return
	}(c)

	if err != nil {
		logger.Log.Debugf("[%s] problem parsing: %s", message, err.Error())
		c.JSON(http.StatusOK, gin.H{"message": err.Error(), "success": false})
	} else {
		c.JSON(http.StatusOK, gin.H{"message": message, "success": true})
	}
}

func handlerReverseSettings(c *gin.Context) {
	message, err := func(c *gin.Context) (message string, err error) {
		// bind sensor data
		type ReverseSettings struct {
			// Minimum number of passive
			MinimumPassive int `json:"minimum_passive"`
			// Timespan of window
			Window int64 `json:"window"`
			// Family is a group of devices
			Family string `json:"family" binding:"required"`
			// Device are unique within a family
			Device string `json:"device"`
			// Location is optional, used for designating learning
			Location string `json:"location"`
			// Latitude
			Latitude float64 `json:"lat"`
			// Longitude
			Longitude float64 `json:"lon"`
			// Altitude
			Altitude float64 `json:"alt"`
		}
		var d ReverseSettings
		err = c.BindJSON(&d)
		if err != nil {
			err = errors.Wrap(err, "could not bind json")
			return
		}
		d.Family = strings.TrimSpace(strings.ToLower(d.Family))
		d.Device = strings.TrimSpace(strings.ToLower(d.Device))
		d.Location = strings.TrimSpace(strings.ToLower(d.Location))

		var rollingData models.ReverseRollingData
		err = db.Get("ReverseRollingData", &rollingData)
		if err != nil {
			rollingData = models.ReverseRollingData{
				Family:         d.Family,
				DeviceLocation: make(map[string]string),
				DeviceGPS:      make(map[string]models.GPS),
				TimeBlock:      90 * time.Second,
			}
		}
		if rollingData.TimeBlock.Seconds() == 0 {
			rollingData.TimeBlock = 90 * time.Second
		}

		// set tracking information
		if d.Device != "" {
			if d.Location != "" {
				message = fmt.Sprintf("Set location to '%s' for %s for learning with device '%s'", d.Location, d.Family, d.Device)
				rollingData.DeviceLocation[d.Device] = d.Location
				if d.Latitude != 0 && d.Longitude != 0 {
					rollingData.DeviceGPS[d.Device] = models.GPS{
						Latitude:  d.Latitude,
						Longitude: d.Longitude,
						Altitude:  d.Altitude,
					}
				}
			} else {
				message = fmt.Sprintf("switched to tracking for %s", d.Family)
				delete(rollingData.DeviceLocation, d.Device)
			}
			message += ". "
		}
		message += fmt.Sprintf("Now learning on %d devices: %+v", len(rollingData.DeviceLocation), rollingData.DeviceLocation)

		// set time block information
		if d.Window > 0 {
			rollingData.TimeBlock = time.Duration(d.Window) * time.Second
		}
		message += fmt.Sprintf("with time block of %2.0f seconds", rollingData.TimeBlock.Seconds())

		if d.MinimumPassive != 0 {
			rollingData.MinimumPassive = d.MinimumPassive
			message += fmt.Sprintf(" and set minimum passive to %d", rollingData.MinimumPassive)
		}

		err = db.Set("ReverseRollingData", rollingData)
		logger.Log.Debugf("[%s] %s", d.Family, message)
		return
	}(c)

	if err != nil {
		logger.Log.Warn(err)
		c.JSON(http.StatusOK, gin.H{"message": err.Error(), "success": false})
	} else {
		c.JSON(http.StatusOK, gin.H{"message": message, "success": true})
	}
}

func handlerReverse(c *gin.Context) {
	message, err := func(c *gin.Context) (message string, err error) {
		// bind sensor data
		var d models.SensorData
		err = c.BindJSON(&d)
		if err != nil {
			logger.Log.Warn(err)
			return
		}

		// validate sensor data
		err = d.Validate()
		if err != nil {
			logger.Log.Warn(err)
			return
		}

		d.Family = strings.TrimSpace(strings.ToLower(d.Family))

		if d.Location != "" {
			logger.Log.Debugf("[%s] entered passive fingerprint for %s at %s", d.Family, d.Device, d.Location)
		} else {
			logger.Log.Debugf("[%s] entered passive fingerprint for %s", d.Family, d.Device)
		}

		var rollingData models.ReverseRollingData
		err = db.Get("ReverseRollingData", &rollingData)
		if err != nil {
			// defaults
			rollingData = models.ReverseRollingData{
				Family:         d.Family,
				DeviceLocation: make(map[string]string),
				TimeBlock:      90 * time.Second,
			}
		}
		if rollingData.TimeBlock.Seconds() == 0 {
			rollingData.TimeBlock = 90 * time.Second
		}

		if !rollingData.HasData {
			rollingData.Timestamp = time.Now().UTC()
			rollingData.Datas = []models.SensorData{}
			rollingData.HasData = true
		}
		if len(d.Sensors) == 0 {
			err = errors.New("no fingerprints")
			return
		}

		rollingData.Datas = append(rollingData.Datas, d)
		numFingerprints := 0
		for sensor := range d.Sensors {
			numFingerprints += len(d.Sensors[sensor])
		}
		err = db.Set("ReverseRollingData", rollingData)
		message = fmt.Sprintf("inserted %d fingerprints for %s", numFingerprints, d.Family)

		if err == nil {
			go parseRollingData(d.Family)
		}
		return
	}(c)

	if err != nil {
		logger.Log.Warn(err)
		c.JSON(http.StatusOK, gin.H{"message": err.Error(), "success": false})
	} else {
		c.JSON(http.StatusOK, gin.H{"message": message, "success": true})
	}

}

func parseRollingData(family string) (err error) {

	var rollingData models.ReverseRollingData
	err = db.Get("ReverseRollingData", &rollingData)
	if err != nil {
		return
	}

	sensorMap := make(map[string]models.SensorData)
	if rollingData.HasData && time.Since(rollingData.Timestamp) > rollingData.TimeBlock {
		logger.Log.Debugf("[%s] New data arrived %s", family, time.Since(rollingData.Timestamp))
		// merge data
		for _, data := range rollingData.Datas {
			for sensor := range data.Sensors {
				for mac := range data.Sensors[sensor] {
					rssi := data.Sensors[sensor][mac]
					trackedDeviceName := sensor + "-" + mac
					if _, ok := sensorMap[trackedDeviceName]; !ok {
						location := ""
						// if there is a device+location in map, then it is currently doing learning
						if loc, hasMac := rollingData.DeviceLocation[trackedDeviceName]; hasMac {
							location = loc
						}
						var gps models.GPS
						if g, hasMac := rollingData.DeviceGPS[trackedDeviceName]; hasMac {
							gps = g
						}
						sensorMap[trackedDeviceName] = models.SensorData{
							Family:    family,
							Device:    trackedDeviceName,
							Timestamp: time.Now().UTC().UnixNano() / int64(time.Millisecond),
							Sensors:   make(map[string]map[string]interface{}),
							Location:  location,
							GPS:       gps,
						}
						time.Sleep(10 * time.Millisecond)
						sensorMap[trackedDeviceName].Sensors[sensor] = make(map[string]interface{})
					}
					sensorMap[trackedDeviceName].Sensors[sensor][data.Device+"-"+sensor] = rssi
				}
			}
		}
		rollingData.HasData = false
	}
	db.Set("ReverseRollingData", rollingData)
	db.Close()
	for sensor := range sensorMap {
		logger.Log.Debugf("[%s] reverse sensor data: %+v", family, sensorMap[sensor])
		numPassivePoints := 0
		for sensorType := range sensorMap[sensor].Sensors {
			numPassivePoints += len(sensorMap[sensor].Sensors[sensorType])
		}
		if numPassivePoints < rollingData.MinimumPassive {
			logger.Log.Debugf("[%s] skipped saving reverse sensor data for %s, not enough points (< %d)", family, sensor, rollingData.MinimumPassive)
			continue
		}
		err := processSensorData(sensorMap[sensor])
		if err != nil {
			logger.Log.Warnf("[%s] problem saving: %s", family, err.Error())
		}
		logger.Log.Debugf("[%s] saved reverse sensor data for %s", family, sensor)
	}

	return
}

func processSensorData(p models.SensorData, justSave ...bool) (err error) {
	if err = api.SaveSensorData(p, db); err != nil {
		return
	}

	if len(justSave) < 0 || !justSave[0] {
		go sendOutData(p)
	}

	return
}

func sendOutData(p models.SensorData) (analysis models.LocationAnalysis, err error) {
	analysis, _ = api.AnalyzeSensorData(p, db)
	if len(analysis.Guesses) == 0 {
		err = errors.New("no guesses")
		return
	}
	type Payload struct {
		Sensors  models.SensorData           `json:"sensors"`
		Guesses  []models.LocationPrediction `json:"guesses"`
		Location string                      `json:"location"` // FIND backwards-compatability
		Time     int64                       `json:"time"`     // FIND backwards-compatability
	}

	// determine GPS coordinates
	gpsData, err := api.GetGPSData(p.Family)
	_, hasLoc := gpsData[analysis.Guesses[0].Location]
	if err == nil && hasLoc {
		p.GPS.Latitude = gpsData[analysis.Guesses[0].Location].GPS.Latitude
		p.GPS.Longitude = gpsData[analysis.Guesses[0].Location].GPS.Longitude
	} else {
		p.GPS.Latitude = -1
		p.GPS.Longitude = -1
	}

	payload := Payload{
		Sensors:  p,
		Guesses:  analysis.Guesses,
		Location: analysis.Guesses[0].Location,
		Time:     p.Timestamp,
	}

	bTarget, err := json.Marshal(payload)
	if err != nil {
		return
	}

	p.Family = strings.TrimSpace(strings.ToLower(p.Family))

	// logger.Log.Debugf("sending data over websockets (%s/%s):%s", p.Family, p.Device, bTarget)
	SendMessageOverWebsockets(p.Family, p.Device, bTarget)
	SendMessageOverWebsockets(p.Family, "all", bTarget)

	if UseMQTT {
		logger.Log.Debugf("[%s] sending data over mqtt (%s)", p.Family, p.Device)
		mqtt.Publish(p.Family, p.Device, string(bTarget))
	}
	return
}

func middleWareHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		t := time.Now().UTC()
		// Add base headers
		addCORS(c)
		// Run next function
		c.Next()
		// Log request
		logger.Log.Infof("%v %v %v %s", c.Request.RemoteAddr, c.Request.Method, c.Request.URL, time.Since(t))
	}
}

func addCORS(c *gin.Context) {
	c.Writer.Header().Set("Access-Control-Allow-Origin", "*")
	c.Writer.Header().Set("Access-Control-Max-Age", "86400")
	c.Writer.Header().Set("Access-Control-Allow-Methods", "GET")
	c.Writer.Header().Set("Access-Control-Allow-Headers", "Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization, X-Max")
	c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
}
