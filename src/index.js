const axios = require('axios')
const moment = require('moment')
require('dotenv').config()

var CronJob = require('cron').CronJob;

let CEMADEN_TOKEN = ''
let CEMADEN_EXPIRE
let SIBH_STATIONS = {}
let MEASUREMENTS = []
let STATION_LAST_MEASUREMENT = {}

const database_user = process.env.DATABASE_USER;
const database_pass = process.env.DATABASE_PASS;
const database_addr = process.env.DATABASE_ADDR;
const database_port = process.env.DATABASE_PORT;
const database_name = process.env.DATABASE_NAME;
const cemaden_email = process.env.CEMADEN_EMAIL;
const cemaden_pass = process.env.CEMADEN_PASS;
const cron_pattern = process.env.CRON_PATTERN;

const pgp = require('pg-promise')({
    /* initialization options */
    capSQL: true // capitalize all generated SQL
});

const db = pgp({
    connectionString: 'postgres://'+database_user+':'+database_pass+'@'+database_addr+':'+database_port+'/'+database_name
});

const cs = new pgp.helpers.ColumnSet(
    ['station_prefix_id','date_hour','value','measurement_classification_type_id','transmission_type_id','information_origin'],
    {table: 'measurements'}
);

var job_default = new CronJob(
    cron_pattern,
	function() {
        start()
	},
	null,
	true
);


const start = async () =>{
    console.log('começando');
    
    console.log('Buscando postos SIBH');
    await getSibhStations()

    console.log('Autenticando CEMADEN');
    await cemadenAuth()

    if(CEMADEN_TOKEN){
        console.log('Buscando medições CEMADEN');
        let measurements = await getMeasurements()

        console.log('Preparando Medições CEMADEN');
        prepareMeasurements(measurements)    
        
        console.log('Salvando medições, qtd: ', MEASUREMENTS.length);
        
        await saveMeasurements()

        await updateDateLastMeasurement()
        
        return true
    } else {
        console.log('Aguardado próxima tentativa');
        
    }

    
    
}

const saveMeasurements = async () =>{
    if(MEASUREMENTS.length > 0){
        const query = pgp.helpers.insert(MEASUREMENTS, cs) + " ON CONFLICT (date_hour, station_prefix_id, transmission_type_id) DO UPDATE SET value = EXCLUDED.value, information_origin = 'WS-CEMADEN-NODE', updated_at = now() RETURNING station_prefix_id,date_hour;"
        
        await db.any(query).then(ext => {
            // console.log(ext);
            ext.forEach(line=>{
                
                STATION_LAST_MEASUREMENT[line.station_prefix_id] = STATION_LAST_MEASUREMENT[line.station_prefix_id] || line.date_hour
                if(new Date(STATION_LAST_MEASUREMENT[line.station_prefix_id]) < new Date(line.date_hour)){
                    STATION_LAST_MEASUREMENT[line.station_prefix_id] = line.date_hour
                }
            })            
            
            
            console.log(`Medições cadastradas/atualizadas: `, ext.length)
        }).catch(error => {
            console.log("Error Bulk Insert: ", error);
        });
    } else {
        console.log('NENHUMA MEDIÇÃO NA URL');
        
    }
}

const updateDateLastMeasurement = async () =>{
    let data = Object.entries(STATION_LAST_MEASUREMENT).map(([id, date_last_measurement]) => ({id:parseInt(id),  date_last_measurement: moment(date_last_measurement, "YYYY-MM-DD HH:mm").format('YYYY-MM-DD HH:mm') }));

    console.log(data);
    
    if(data && data.length > 0){
        console.log('Atualizando "data da ultima medição" dos postos no SIBH. Qtd: ', data.length)
        
    
        const cs_s = new pgp.helpers.ColumnSet(['id', {name: 'date_last_measurement',cast: "timestamp"}], { table: 'station_prefixes' });
        const query = pgp.helpers.update(data, cs_s) + ' WHERE v.id = t.id';
        
        // Executando a consulta
        await db.query(query)
        .then(() => {
            console.log('data da ultima medição dos postos no SIBH atualizada')
        })
        .catch(error => {
            console.log('Erro ao atualizar "data da ultima medição" dos postos SIBH', error);
        });
        return true
    }
    return true
    
}

const prepareMeasurements = measurements =>{
    MEASUREMENTS = []
    //retorno do webservice é string e nao json
    measurements  = measurements.split('\n')

    console.log('Número de medições diversas encontradas: ', measurements.length);
    
    //primeira linha é warning, segunda é cabeçalho
    for(let i = 2; i < measurements.length; i++){        
        const [prefix,,,,,,date, field, value, offset] = measurements[i].split(';')
        // console.log(prefix, date,field, value, offset);
        if(prefix){      
            let m = mountMeasurement(prefix, date, field, value, offset)
            if(m){
                MEASUREMENTS.push()
            }
            
        }

    }

}

const mountMeasurement = (prefix, date_hour, field, value, offset) =>{
    let station_type_id = field === 'chuva' ? 2 : 'nothing' //nivel ta esquisito, nao estou salvando
    let station = SIBH_STATIONS[`${prefix}_${station_type_id}`]
    
    if(station){
        MEASUREMENTS.push({
            station_prefix_id: station.id,
            // value: station_type_id === 1 ? value * 100 : value,
            value,
            date_hour,
            measurement_classification_type_id: 3,
            transmission_type_id:4,
            information_origin: 'WS-CEMADEN-NODE'
        })
    }

}


const getMeasurements = async () =>{
    
    let start_date = moment().utc().subtract(6, 'hours').format('YYYYMMDDHHmm')
    let end_date = moment().utc().format('YYYYMMDDHHmm')

    let url = `https://sws.cemaden.gov.br/PED/rest/pcds/dados_rede?inicio=${start_date}&fim=${end_date}&uf=sp&rede=11`

    let res = await axios.request({
        method: 'get',
        url: url,
        headers:{
            token: CEMADEN_TOKEN
        }
    })

    return res.data

    
}

const getSibhStations = async () =>{
    let res = await axios.request({
        method: 'GET',
        url: 'https://cth.daee.sp.gov.br/sibh/api/v2/stations?station_owner_ids[]=4'
    })

    res.data.forEach(station=>{
        SIBH_STATIONS[`${station.prefix}_${station.station_type_id}`] = station
    })
}


const cemadenAuth = async  () =>{
    let res 
    try{
        res = await axios.request({
            url:'https://sgaa.cemaden.gov.br/SGAA/rest/controle-token/tokens',
            method:'POST',
            data:{
                email: cemaden_email,
                password: cemaden_pass
            },
        })
        
    } catch(e){
        console.log('Erro na autenticação',e.cause);
        CEMADEN_TOKEN = ''
    }

    if(res?.data){
        CEMADEN_TOKEN = res.data.token
    }

    return res?.data
}

// start()