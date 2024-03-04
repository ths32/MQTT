const mqtt = require('mqtt');
const mysql = require('mysql');
const request = require('request');

const db_host = 'localhost';
const db_user = '';
const db_password = '';
const db_database = '';

const mqtt_options = {host:'', port:, username:'', password:'', reconnectPeriod:, keepalive:};
const mqtt_topic = '#'; //ALL

const request_url = 'http://ip/mqtt?imei=';

let client = mqtt.connect(mqtt_options);
const connection = mysql.createConnection({host:db_host, user:db_user, password:db_password, database:db_database});

client.on('connect', function() {
	console.log('Connect MQTT');
	client.subscribe(mqtt_topic, function(err) {
		console.log('Subscribed to ' + mqtt_topic);
		if (err) console.log(err);
		connection.connect();
	});
});

client.on('message', function(topic, message) {
	let message_str = message.toString();
	console.log(topic, message_str);
	// -------------------------------------------------------------------------------------------------------------------------------------------------------
	// 온도 변환 함수
	function convertTemp(temp){
		temp = (-1 * (6553.5 - parseFloat(temp) + 0.1)).toFixed(2).toString();
		return temp		
	}
	// 원본 데이터 저장용 칼럼 존재 여부 확인 및 미존재시 해당 칼럼 추가 함수
	function checkAddCols(connection, tableName, colName){
		const checkCol = `SELECT count(*) as colCnt FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = 'tbs' AND TABLE_NAME = ? AND COLUMN_NAME = ?`;
		connection.query(mysql.format(checkCol,[tableName,colName]), function(err,results,fields){
			if (err) {
				console.log(err);
			} else {
				if (results[0].colCnt === 0) {
					let createCol;
					// 칼럼 추가
					if (tableName == 'tbmqtt'||tableName == 'tbs_log') {
						createCol = `ALTER TABLE ${tableName} ADD COLUMN ${colName} TEXT`;
						connection.query(createCol,function(err,results,fields){if(err){console.log(err);}});
					} else if (tableName == 'tbs_data'||tableName == 'tbs') {
						createCol = `ALTER TABLE ${tableName} ADD COLUMN ${colName} varchar(10)`;
						connection.query(createCol,function(err,results,fields){if(err){console.log(err);}});
					} 
				} 
			}
		});
	}
	// --------------------------------------------------------------------------------------------------------------------------------------------------------------
	//let temp = parseFloat(message_str.match(/"Temp":\s*"([^"]+)"/)[1]);
	const temp_match = message_str.match(/"Temp":\s*"([^"]+)"/);
	let message_str_ = '';
	if (temp_match && temp_match[1]){
		let temp = parseFloat(temp_match[1]);
		if (!isNaN(temp) && temp >= 6000.0 || !isNaN(temp) && temp >= 6000.00) {
			const temp_ = convertTemp(temp);
			// 이상치 존재
			// (이상치 변경 전) 원본 메시지 저장
			message_str_ = message_str
			// ---------------------------------
			// 메시지의 온도 이상치를 변경
			message_str = message_str.replace(/"Temp":\s*"([^"]+)"/, `"Temp": "${temp_}"`);
			console.log(message_str);
		} else {
			// NORMAL
			message_str_ = message_str
		}
	} else {
		// EMPTY STRING
		message_str_ = message_str
	}
	// -------------------------------------------------------------------------------------------------------------------------------------------------------------------
	// 1. INSERT INTO tbmqtt
	checkAddCols(connection, 'tbmqtt', 'message_raw');
	const sql = 'INSERT INTO tbmqtt (topic, message, created_at, message_raw) VALUES (?,?,NOW(),?)';
	connection.query(mysql.format(sql, [topic, message_str, message_str_]), function(err, results, fields) {if (err) console.log(err);});

	// const sql = 'INSERT INTO tbmqtt (topic, message, created_at) VALUES (?,?,NOW())';
	// connection.query(mysql.format(sql, [topic, message_str]), function(err, results, fields) {
	// 	if (err) console.log(err);
	// });

	if (message_str.includes('DeviceID') && message_str.includes('Bat_CapRema')) {
		const obj = JSON.parse(message_str);

		const now = new Date();
		const data = {
			// device 부분은 배포 후 주석처리 되어 있었음
			device: obj.DeviceID,
			lat: obj.GPS_Latitude,
			lng: obj.GPS_Longitude,
			temp: obj.Temp,
			humi: obj.Humi,
			conduc: obj.Conduc,
			ph: obj.pH,
			nitro: obj.Nitro,
			phos: obj.Phos,
			pota: obj.Pota,
			bat: obj.Bat_CapRema,
		};
		// ------------------------------------------------
		const temp_ = parseInt(data.temp);
		thr = 6000;
		if (!isNaN(temp_) && temp_ > thr) {
			// 온도 변환한 값 저장
			const temp = convertTemp(data.temp);
			// 원본 데이터 온도를 변환된 온도로 변경
			data.temp = temp;
		} 
		// 위의 원본 메시지를 파싱한 후 온도 부분만 추출
		const obj_ = JSON.parse(message_str_);
		const tmp_raw = obj_.Temp;
		// 원본 메시지에서 추출한 원본 온도값을 data에 추가
		const data_ = {
				...data,
				temp_raw: tmp_raw,
		};
		// --------------------------------------------------

		connection.query('SELECT COUNT(*) AS cnt FROM tbs_data WHERE imei=? AND rdate BETWEEN DATE_ADD(NOW(), INTERVAL -3 MINUTE) AND NOW()', [obj.IMEI], function(err, results, fields) {
			if (err) console.log(err);
			else {
				if (results[0].cnt === 0) {
					const tmp = {imei:obj.IMEI, device:obj.DeviceID, device_time:obj.Time, rdate:now};
					// 2. INSERT INTO tbs_data
					checkAddCols(connection, 'tbs_data', 'temp_raw');
					connection.query('INSERT INTO tbs_data SET ?', {...data_, ...tmp}, function(err2, results2, fields2) {if (err2) console.log(err2);});
				}
			}
		});

		if (obj.IMEI && obj.IMEI.length >= 10) {
			// imei가 기등록 되어있는지 확인
			connection.query('SELECT * FROM tbs WHERE imei=? LIMIT 1', [obj.IMEI], function(err, results, fields) {
				if (err) console.log(err);
				else {
					data.device_time = obj.Time;
					// UPDATE tbs
					if (results.length > 0) {
						data.updated_at = now;
						// update data_s
						const data_s = {
							...data,
							temp_raw: tmp_raw,
						};
						checkAddCols(connection, 'tbs', 'temp_raw');
						connection.query('UPDATE tbs SET ? WHERE imei=?', [data_s, obj.IMEI], function(err2, results2, fields2) {if (err2) console.log(err2);});
						// --------------------------------------------------------------------------------------------------------------------------
						// connection.query('UPDATE tbs SET ? WHERE imei=?', [data, obj.IMEI], function(err2, results2, fields2) {
						// 	if (err2) console.log(err2);
						// });
					}
					else {
						// 3. INSERT INTO tbs
						data.imei = obj.IMEI;
						data.created_at = now;
						// insert data_s 
						const data_s = {
							...data,
							temp_raw: tmp_raw,
						};
						checkAddCols(connection, 'tbs', 'temp_raw');
						connection.query('INSERT INTO tbs SET ?', data_s, function(err2, results2, fields2) {if (err2) console.log(err2);});
						// connection.query('INSERT INTO tbs SET ?', data, function(err2, results2, fields2) {
						// 	if (err2) console.log(err2);
						// });
					}
				}
			});
			
			request.get({url:request_url+obj.IMEI});
		}
		// 4. INSERT INTO tbs_log
		checkAddCols(connection, 'tbs_log', 'message_raw');
		connection.query('INSERT INTO tbs_log SET ?', {imei:obj.IMEI, device:obj.DeviceID, message:message_str, rdate:now, message_raw:message_str_}, function(err, results, fields) {if (err) console.log(err);});
		// -------------------------------------------------------------------------------------------------------------------------------------------------------
		// connection.query('INSERT INTO tbs_log SET ?', {imei:obj.IMEI, device:obj.DeviceID, message:message_str, rdate:now}, function(err, results, fields) {
		// 	if (err) console.log(err);
		// });
	}
});

client.on('error', function(err) {
	console.log('Error MQTT:', err);
	client.end();
	client.reconnect();
});

client.on('reconnect', function(err) {
	console.log('Reconnect MQTT');
	client = null;
	client = mqtt.connect(mqtt_options);
});

client.on('close', function() {
	console.log('Close MQTT');
	client.end();
	client.reconnect();
});

//connection.end(); //MySQL 종료
