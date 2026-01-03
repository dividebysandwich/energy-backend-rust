use std::io::prelude::*;
use std::net::TcpStream;
use std::fs::OpenOptions;
use std::io::Write;
use std::env;
use ssh2::Session;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Result;
use regex::Regex;
use std::time::SystemTime;
use std::thread;
use std::time;
use chrono::Local;
use std::fs::File;
use std::io::{BufReader, BufRead, Error, ErrorKind, Result as IOResult};
#[allow(unused_mut)]

#[derive(Serialize, Deserialize)]
struct EnergyData {
    pub time: u128,
    #[serde(rename = "Grid")]
    pub grid: f64,
    #[serde(rename = "PV")]
    pub pv: f64,
    #[serde(rename = "Consumption")]
    pub consumption: f64,
    #[serde(rename = "Efficiency")]
    pub efficiency: f64,
    #[serde(rename = "Losses")]
    pub losses: f64,
    #[serde(rename = "ActualConsumption")]
    pub actual_consumption: f64,
    #[serde(rename = "BatterySOC")]
    pub battery_soc: f64,
    #[serde(rename = "BatteryVoltage")]
    pub battery_voltage: f64,
    #[serde(rename = "BatteryCurrent")]
    pub battery_current: f64,
    #[serde(rename = "BatteryPower")]
    pub battery_power: f64,
    #[serde(rename = "GridVoltL1")]
    pub grid_voltage_l1: f64,
    #[serde(rename = "GridVoltL2")]
    pub grid_voltage_l2: f64,
    #[serde(rename = "GridVoltL3")]
    pub grid_voltage_l3: f64,
    #[serde(rename = "GridPowerL1")]
    pub grid_power_l1: f64,
    #[serde(rename = "GridPowerL2")]
    pub grid_power_l2: f64,
    #[serde(rename = "GridPowerL3")]
    pub grid_power_l3: f64,
    #[serde(rename = "GridConsumptionL1")]
    pub consumption_l1: f64,
    #[serde(rename = "GridConsumptionL2")]
    pub consumption_l2: f64,
    #[serde(rename = "GridConsumptionL3")]
    pub consumption_l3: f64,
    #[serde(rename = "GridForwardL1")]
    pub grid_forward_l1: f64,
    #[serde(rename = "GridForwardL2")]
    pub grid_forward_l2: f64,
    #[serde(rename = "GridForwardL3")]
    pub grid_forward_l3: f64,
    #[serde(rename = "GridReverseL1")]
    pub grid_reverse_l1: f64,
    #[serde(rename = "GridReverseL2")]
    pub grid_reverse_l2: f64,
    #[serde(rename = "GridReverseL3")]
    pub grid_reverse_l3: f64,
    #[serde(rename = "TemperatureBatteryRoom")]
    pub temperature_battery_room: f64,
    #[serde(rename = "HumidityBatteryRoom")]
    pub humidity_battery_room: f64,
    #[serde(rename = "TemperatureOutside")]
    pub temperature_outside: f64,
    #[serde(rename = "HumidityOutside")]
    pub humidity_outside: f64,
    #[serde(rename = "Temperature1")]
    pub temperature_1: f64,
    #[serde(rename = "Humidity1")]
    pub humidity_1: f64,
    #[serde(rename = "Temperature2")]
    pub temperature_2: f64,
    #[serde(rename = "Humidity2")]
    pub humidity_2: f64,
}

#[derive(Serialize, Deserialize)]
struct CompactEnergyData {
    pub time: String,
    pub date: String,
    pub soc: i32,
    pub pv: f64,
    pub consumption: f64,
    pub grid: f64,
    pub batteryuse: f64,
    pub soc_histogram: Vec<f64>,
    pub pv_histogram: Vec<f64>,
    pub consumption_histogram: Vec<f64>,
    pub grid_histogram: Vec<f64>,
    pub battuse_histogram: Vec<f64>,
}

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct InverterData {
    #[serde(rename = "Ac/ActiveIn/Source")]
    pub Ac_ActiveIn_Source: f64,
    #[serde(rename = "Ac/Consumption/L1/Power")]
    pub Ac_Consumption_L1_Power: f64,
    #[serde(rename = "Ac/Consumption/L2/Power")]
    pub Ac_Consumption_L2_Power: f64,
    #[serde(rename = "Ac/Consumption/L3/Power")]
    pub Ac_Consumption_L3_Power: f64,
    #[serde(rename = "Ac/Grid/L1/Power")]
    pub Ac_Grid_L1_Power: f64,
    #[serde(rename = "Ac/Grid/L2/Power")]
    pub Ac_Grid_L2_Power: f64,
    #[serde(rename = "Ac/Grid/L3/Power")]
    pub Ac_Grid_L3_Power: f64,
    #[serde(rename = "Ac/PvOnGrid/L1/Power", default, deserialize_with = "deserialize_float_or_empty_as_option")]
    pub Ac_PvOnGrid_L1_Power: Option<f64>,
    #[serde(rename = "Ac/PvOnGrid/L2/Power", default, deserialize_with = "deserialize_float_or_empty_as_option")]
    pub Ac_PvOnGrid_L2_Power:  Option<f64>,
    #[serde(rename = "Ac/PvOnGrid/L3/Power", default, deserialize_with = "deserialize_float_or_empty_as_option")]
    pub Ac_PvOnGrid_L3_Power:  Option<f64>,
    #[serde(rename = "Dc/Battery/Soc")]
    pub Dc_Battery_Soc: f64,
    #[serde(rename = "Dc/Battery/Power")]
    pub Dc_Battery_Power: f64,
    #[serde(rename = "Dc/Battery/Voltage")]
    pub Dc_Battery_Voltage: f64,
    #[serde(rename = "Dc/Battery/Current")]
    pub Dc_Battery_Current: f64,
}

#[derive(Serialize, Deserialize)]
#[allow(non_snake_case)]
struct MeterData {
    #[serde(rename = "Ac/L1/Voltage")]
    pub Ac_L1_Voltage: f64,
    #[serde(rename = "Ac/L2/Voltage")]
    pub Ac_L2_Voltage: f64,
    #[serde(rename = "Ac/L3/Voltage")]
    pub Ac_L3_Voltage: f64,
    #[serde(rename = "Ac/L1/Power")]
    pub Ac_L1_Power: f64,
    #[serde(rename = "Ac/L2/Power")]
    pub Ac_L2_Power: f64,
    #[serde(rename = "Ac/L3/Power")]
    pub Ac_L3_Power: f64,
    #[serde(rename = "Ac/L1/Energy/Forward")]
    pub Ac_L1_Energy_Forward: f64,
    #[serde(rename = "Ac/L2/Energy/Forward")]
    pub Ac_L2_Energy_Forward: f64,
    #[serde(rename = "Ac/L3/Energy/Forward")]
    pub Ac_L3_Energy_Forward: f64,
    #[serde(rename = "Ac/L1/Energy/Reverse")]
    pub Ac_L1_Energy_Reverse: f64,
    #[serde(rename = "Ac/L2/Energy/Reverse")]
    pub Ac_L2_Energy_Reverse: f64,
    #[serde(rename = "Ac/L3/Energy/Reverse")]
    pub Ac_L3_Energy_Reverse: f64,
}

fn deserialize_float_or_empty_as_option<'de, D>(deserializer: D) -> std::result::Result<Option<f64>, D::Error>
where
    D: Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum FloatOrEmpty {
        Float(f64),
        Empty(Vec<()>),
    }

    match FloatOrEmpty::deserialize(deserializer)? {
        FloatOrEmpty::Float(f) => Ok(Some(f)),
        FloatOrEmpty::Empty(_) => Ok(None),
    }
}

fn get_sys_time_in_msecs() -> u128 {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_millis(),
        Err(_) => panic!("SystemTime before Unix epoch!"),
    }
}

fn write_value_to_file(filename: &str, value: &str, append: bool) {

    let mut file = OpenOptions::new()
        .write(true)
        .append(append)
        .truncate(!append)
        .create(true)
        .open(filename)
        .unwrap();

    file.write_all(value.as_bytes()).unwrap();
    file.write_all("\n".as_bytes()).unwrap();
}

// Convert python dict string to a json string
fn dict_to_json(json: &str) -> String {

    // Step 1: Join all lines into one line
    let single_line = json
        .lines()
        .map(str::trim)
        .collect::<Vec<_>>()
        .join(" ");

    // Step 2: Remove trailing commas (optional but helps for safety)
    let cleaned = single_line.trim_end_matches(',');

    // Step 3: Convert Python-style dict to JSON-compatible format
    // - Replace single quotes with double quotes
    // - Collapse adjacent quoted words into one string
    let re_multiline_str = Regex::new(r"'([^']*?)'\s+'").unwrap();
    let mut intermediate = cleaned.to_string();

    // Collapse `'foo' 'bar'` into `'foo bar'`
    while re_multiline_str.is_match(&intermediate) {
        intermediate = re_multiline_str
            .replace_all(&intermediate, "'$1 ")
            .to_string();
    }

    // Replace all single quotes with double quotes
    let json_like = intermediate.replace('\'', "\"");
    let mut fixed_json = json_like.replace("\"{\"", "{\"");
    fixed_json = fixed_json.replace("}\"", "}");
    fixed_json = fixed_json.replace("value = ", "");
    fixed_json = fixed_json.replace(": True", ": \"TRUE\"");
    fixed_json = fixed_json.replace(": False", ": \"FALSE\"");
    fixed_json = fixed_json.replace("\"}\",", "\"},");

    // Wrap as object (if needed)
//    let wrapped_json = format!("{{{}}}", json_like);

    fixed_json
}


fn read_first_line_as_float(filename: &str) -> IOResult<f64> {
    let file = File::open(filename)?;
    let reader = BufReader::new(file);
    
    // Read the first line
    let first_line = match reader.lines().next() {
        Some(Ok(line)) => line,
        _ => return Err(Error::new(ErrorKind::Other, "File is empty")),
    };

    // Check if the first line equals "None"
    if first_line.trim() == "None" {
        Ok(0.0)
    } else {
        // Parse the first line as a float
        match first_line.trim().parse::<f64>() {
            Ok(value) => Ok(value),
            Err(_) => Err(Error::new(ErrorKind::InvalidData, "Invalid float value")),
        }
    }
}

fn read_history_from_file(filename: &str) -> Vec<f64> {
    // Open the file
    let file = File::open(filename).unwrap();
    let reader = BufReader::new(file);

    // Iterate over each line in the file
    let mut result: Vec<f64> = Vec::new();
    for line in reader.lines() {
        // Parse each line into a f64
        if let Ok(num) = line.unwrap().trim().parse::<f64>() {
            result.push(num);
        } else {
            println!("Warning: Could not parse line in file {}", filename);
        }
    }
    result
}

fn fetch_and_process(sess: Session) {
    let mut channel1 = sess.channel_session().unwrap();
    channel1.exec("nice -n 10 dbus -y com.victronenergy.system / GetValue").unwrap();
    let mut inverter_data_raw = String::new();
    channel1.read_to_string(&mut inverter_data_raw).unwrap();
    println!("{}", dict_to_json(&inverter_data_raw));
    let _ = channel1.wait_close();

    let res1: Result<InverterData> = serde_json::from_str(dict_to_json(&inverter_data_raw).as_str());
    match res1 {
        Ok(inverter_result) => {
            let mut channel2 = sess.channel_session().unwrap();
            channel2.exec("nice -n 10 dbus -y com.victronenergy.grid.cgwacs_ttyUSB0_mb1 / GetValue").unwrap();
            let mut energymeter_data_raw = String::new();
            channel2.read_to_string(&mut energymeter_data_raw).unwrap();
        //    println!("{}", energymeter_data_raw);
            let _ = channel2.wait_close();
            let energymeter_data = dict_to_json(&energymeter_data_raw);
            let res2: Result<MeterData> = serde_json::from_str(&energymeter_data);
            match res2 {
                Ok(meter_result) => {
                    // Now we have both inverter and energy meter results, so we can process the data

                    let mut value_pv = inverter_result.Ac_PvOnGrid_L1_Power.unwrap_or(0.0) + inverter_result.Ac_PvOnGrid_L2_Power.unwrap_or(0.0) + inverter_result.Ac_PvOnGrid_L3_Power.unwrap_or(0.0);
                    let mut value_grid = inverter_result.Ac_Grid_L1_Power + inverter_result.Ac_Grid_L2_Power + inverter_result.Ac_Grid_L3_Power;
                    if value_pv < 0.0 {
                        value_pv = 0.0;
                    }
                    let mut value_consumption = inverter_result.Ac_Consumption_L1_Power + inverter_result.Ac_Consumption_L2_Power + inverter_result.Ac_Consumption_L3_Power;
                    let mut value_efficiency = 100.0;
                    let mut value_losses = 0.0;
                    if value_pv > value_consumption {
                        let delta = value_pv - inverter_result.Dc_Battery_Power - value_consumption;
                        if delta > 0.0 {
                            value_efficiency = 100.0 / value_pv * (inverter_result.Dc_Battery_Power + value_consumption);
                            value_losses = delta;
                        }
                    } else if inverter_result.Dc_Battery_Power < 0.0 {
                        let delta = -inverter_result.Dc_Battery_Power - value_consumption - value_pv;
                        if delta > 0.0 {
                            value_efficiency = 100.0 / -inverter_result.Dc_Battery_Power * (value_pv + value_consumption);
                            value_losses = delta;
                        }
                    }

                    //When feeding into the grid, the system adds the grid feed to the consumption so we need to substract
                    if value_grid < 0.0 {
                        // using addition because value_grid will be negative
                        value_consumption = value_consumption + value_grid;
                    }


                    let path_to_files = "/var/www/html/status/";
                    //let path_to_files = "./";

                    let mut temperature_1 = 0.0;
                    let mut temperature_2 = 0.0;
                    let mut temperature_i = 0.0;
                    let mut temperature_o = 0.0;
                    let mut humidity_1 = 0.0;
                    let mut humidity_2 = 0.0;
                    let mut humidity_i = 0.0;
                    let mut humidity_o = 0.0;

                    //Enable this if you have temperature sensors available
//                    let temperature_1 = trim(file_get_contents('/var/www/html/status/temp_1.txt'));
//                    let humidity_1 = trim(file_get_contents('/var/www/html/status/humi_1.txt'));
//                    let temperature2 = trim(file_get_contents('/var/www/html/status/temp_2.txt'));
//                    let humidity2 = trim(file_get_contents('/var/www/html/status/humi_2.txt'));
//                    let temperature_i = trim(file_get_contents('/var/www/html/status/tempi.txt'));
//                    let humidity_i = trim(file_get_contents('/var/www/html/status/humii.txt'));
//                    let temperature_o = trim(file_get_contents('/var/www/html/status/tempo.txt'));
//                    let humidity_o = trim(file_get_contents('/var/www/html/status/humio.txt'));

                    match read_first_line_as_float(format!("{}temp_1.txt", &path_to_files).as_str()) {
                        Ok(line) => temperature_1 = line,
                        Err(_) => {},
                    }

                    let mut consumption = value_consumption;
                    // Charging the battery is included in the AC load figure on systems where the ESS is externally controlled by the PV inverter
                    if inverter_result.Dc_Battery_Power > 0.0 {
                        consumption = value_consumption - inverter_result.Dc_Battery_Power;
                    }

                    let energy_data = EnergyData {
                        time: get_sys_time_in_msecs(),
                        grid: inverter_result.Ac_Grid_L1_Power + inverter_result.Ac_Grid_L2_Power + inverter_result.Ac_Grid_L3_Power,
                        pv: value_pv,
                        consumption: consumption,
                        efficiency: value_efficiency,
                        losses: value_losses,
                        actual_consumption: consumption,
                        battery_soc: inverter_result.Dc_Battery_Soc,
                        battery_voltage: inverter_result.Dc_Battery_Voltage,
                        battery_current: inverter_result.Dc_Battery_Current,
                        battery_power: inverter_result.Dc_Battery_Power,
                        grid_voltage_l1: meter_result.Ac_L1_Voltage,
                        grid_voltage_l2: meter_result.Ac_L2_Voltage,
                        grid_voltage_l3: meter_result.Ac_L3_Voltage,
                        grid_power_l1: meter_result.Ac_L1_Power,
                        grid_power_l2: meter_result.Ac_L2_Power,
                        grid_power_l3: meter_result.Ac_L3_Power,
                        consumption_l1: meter_result.Ac_L1_Power,
                        consumption_l2: meter_result.Ac_L2_Power,
                        consumption_l3: meter_result.Ac_L3_Power,
                        grid_forward_l1: meter_result.Ac_L1_Energy_Forward,
                        grid_forward_l2: meter_result.Ac_L2_Energy_Forward,
                        grid_forward_l3: meter_result.Ac_L3_Energy_Forward,
                        grid_reverse_l1: meter_result.Ac_L1_Energy_Reverse,
                        grid_reverse_l2: meter_result.Ac_L2_Energy_Reverse,
                        grid_reverse_l3: meter_result.Ac_L3_Energy_Reverse,
                        temperature_battery_room: temperature_i,
                        humidity_battery_room: humidity_i,
                        temperature_outside: temperature_o,
                        humidity_outside: humidity_o,
                        temperature_1: temperature_1,
                        humidity_1: humidity_1,
                        temperature_2: temperature_2,
                        humidity_2: humidity_2,
                    };
                    let current_time = Local::now();
                    // Store data in Elasticsearch
                    let result = serde_json::to_string(&energy_data);
                    match result {
                        Ok(json_result) => {
//                            println!("Data: {}", json_result);
                            let client = reqwest::blocking::Client::new();
                            match client.post("http://elasticsearch:9200/power/_doc")
                                .header(reqwest::header::CONTENT_TYPE, "application/json")
                                .header(reqwest::header::CONTENT_LENGTH, json_result.len())
                                .basic_auth("user", Some("pass"))
                                .body(json_result)
                                .timeout(std::time::Duration::from_secs(10))
                                .send() {
                                Ok(_) => {
                                }
                                Err(e) => {
                                    println!("Error: {}", e);
                                }
                            }

                        }
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }


                    let compact_energy_data = CompactEnergyData {
                        time: current_time.format("%H:%M").to_string(),
                        date: current_time.format("%e %B %Y").to_string(),
                        soc: energy_data.battery_soc.round() as i32,
                        pv: format!("{:.1}", energy_data.pv/1000.0).parse().unwrap(),
                        consumption: format!("{:.1}", energy_data.actual_consumption/1000.0).parse().unwrap(),
                        grid: format!("{:.1}", energy_data.grid/1000.0).parse().unwrap(),
                        batteryuse: format!("{:.1}", energy_data.battery_power/1000.0).parse().unwrap(),
                        // These history files contain the average of the last 24 hours
                        // You can use them to plot graphs and you'll have to create them yourself, or using the vrm_histogram.php script
                        // in the energy_backend repository. Or you could replace this with something from elasticsearch.
                        soc_histogram: read_history_from_file(format!("{}lastbattsoc.txt", &path_to_files).as_str()),
                        pv_histogram: read_history_from_file(format!("{}lastpv.txt", &path_to_files).as_str()),
                        consumption_histogram: read_history_from_file(format!("{}lastuse.txt", &path_to_files).as_str()),
                        grid_histogram: read_history_from_file(format!("{}lastgrid.txt", &path_to_files).as_str()),
                        battuse_histogram: read_history_from_file(format!("{}lastbattuse.txt", &path_to_files).as_str()),
                    };

                    // Send compact data to a small redistribution service
                    // This could be a small REST endpoint that takes this data and regurgitates it on request.
                    let result = serde_json::to_string(&compact_energy_data);
                    match result {
                        Ok(json_result) => {
                            println!("CompactEnergy: {}", json_result);
                            let client = reqwest::blocking::Client::new();
                            match client.post("https://yourserver.com/updateEnergy")
                                .header(reqwest::header::CONTENT_TYPE, "application/json")
                                .header(reqwest::header::CONTENT_LENGTH, json_result.len())
                                .body(json_result)
                                .timeout(std::time::Duration::from_secs(10))
                                .send() {
                                Ok(_) => {
                                }
                                Err(e) => {
                                    println!("Error: {}", e);
                                }
                            }
                        }
                        Err(e) => {
                            println!("Error: {}", e);
                        }
                    }

                    // This is a single file containing just the most important current values
                    write_value_to_file(format!("{}soc.txt", &path_to_files).as_str(), (format!("{:.0}", energy_data.battery_soc)).as_str(), false);
                    write_value_to_file(format!("{}soc.txt", &path_to_files).as_str(), (format!("{:.1}", energy_data.pv/1000.0)).as_str(), true);
                    write_value_to_file(format!("{}soc.txt", &path_to_files).as_str(), (format!("{:.1}", energy_data.actual_consumption/1000.0)).as_str(), true);
                    write_value_to_file(format!("{}soc.txt", &path_to_files).as_str(), (format!("{:.1}", energy_data.grid/1000.0)).as_str(), true);
                    write_value_to_file(format!("{}soc.txt", &path_to_files).as_str(), (format!("{:.1}", energy_data.battery_power/1000.0)).as_str(), true);
                    write_value_to_file(format!("{}soc.txt", &path_to_files).as_str(), (current_time.format("%H:%M").to_string()).as_str(), true);
                    write_value_to_file(format!("{}soc.txt", &path_to_files).as_str(), current_time.format("%e %B %Y").to_string().as_str(), true);
                    println!("Energy loop done: {}", current_time.format("%H:%M:%S").to_string());
                    // These are individual history files that grow over time. Each minute, a cronjob calculates the average and stores that into another history file.
                    write_value_to_file(format!("{}pv_w.txt", &path_to_files).as_str(), energy_data.pv.round().to_string().as_str(), true);
                    write_value_to_file(format!("{}use_w.txt", &path_to_files).as_str(), energy_data.actual_consumption.round().to_string().as_str(), true);
                    write_value_to_file(format!("{}grid_w.txt", &path_to_files).as_str(), energy_data.grid.round().to_string().as_str(), true);
                    write_value_to_file(format!("{}battsoc_w.txt", &path_to_files).as_str(), energy_data.battery_soc.round().to_string().as_str(), true);
                    write_value_to_file(format!("{}battuse_w.txt", &path_to_files).as_str(), energy_data.battery_power.round().to_string().as_str(), true);

                }
                Err(e) => {
//                    let path = err.path().to_string();
                    println!("Received energy meter data: {}", energymeter_data_raw);
                    println!("Error parsing energy meter data: {}", e);
                }
            }

        }
        Err(e) => {
            println!("Received inverter meter data: {}", inverter_data_raw);
//            let path = err.path().to_string();
                    println!("Error parsing inverter data: {}", e);
        }
    }
}



fn main() {
    ctrlc::set_handler(move || {
        println!("received Ctrl+C!");
        std::process::exit(0);
    }).expect("Error setting Ctrl-C handler");

    let args: Vec<String> = env::args().collect();
    let victron_address = &args[1];
    let victron_username = &args[2];
    let victron_password = &args[3];

    #[allow(while_true)]
    while true {
        // Connect to the Victron's Beaglebone controller
        // Remember to enable root SSH access on the Beaglebone via the Victron GUI
        let tcp = TcpStream::connect(victron_address).unwrap();
        let mut sess = Session::new().unwrap();
        sess.set_tcp_stream(tcp);
        sess.handshake().unwrap();
        sess.userauth_password(&victron_username, &victron_password).unwrap();
        #[allow(while_true)]
        while true {
            fetch_and_process(sess.clone());
            thread::sleep(time::Duration::from_secs(2));
        }
    }

//    println!("{}", channel.exit_status().unwrap());
}

