const {
    readFileSync,
    mkdir,
    existsSync,
    mkdirSync,
    writeFileSync,
    appendFileSync,
    appendFile,
    readdirSync,
    rmdirSync
} = require('fs');
require('dotenv').config()

const cliProgress = require('cli-progress');
const opt = cliProgress.Presets.shades_classic;
opt.format = 'progress [{bar}] {percentage}% | ETA: {eta}s | DURATION: {duration}s | {value}/{total} '
const BAR = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);


const axios = require('axios');
const {Client} = require('@elastic/elasticsearch')
const {config} = require("dotenv");

const https = require('https');
const logPath = __dirname + "/../log.csv";
const logCliPath = __dirname + "/../log_cli.log";
writeFileSync(logPath, ['STATUS', 'FROM', 'TO'].join(";") + "\r\n");
writeFileSync(logCliPath, "");

const myArgs = process.argv.slice(2);
const FROM = parseInt(myArgs[0] + "")
const TO = parseInt(myArgs[1] + "")

if (Number.isNaN(FROM) || Number.isNaN(TO)) throw new Error("invalid from or to in env")
if (FROM % 200 !== 0) throw new Error("FROM MUST BE MULTIPLE OF 200")
if (TO % 200 !== 0) throw new Error("TO MUST BE MULTIPLE OF 200")


const TOTAL = parseInt(process.env.TOTAL);
const CONCURRENCY_STEP = parseInt(process.env.CONCURRENCY_STEP);
const STEP_BY = parseInt(process.env.STEP_BY);

const REQUEST_LIMIT = parseInt(process.env.REQUEST_LIMIT);
const CONCURRENCY_REQUEST_FOR_EACH_STEP = parseInt(process.env.CONCURRENCY_REQUEST_FOR_EACH_STEP);


const START_ALL_P = new Date().getTime();

const DIRECTUS_API = axios.create({
    baseURL: getTargetBaseUrl(process.env.DIRECTUS_HOST, process.env.DIRECTUS_PORT),
    httpsAgent: new https.Agent({
        rejectUnauthorized: false
    })
});
const ES_CLIENT = new Client(
    {
        node: 'https://admin:admin@'+process.env.ELASTIC_HOST+':9200',
        ssl: {
            rejectUnauthorized: false,
        },
    }
)


const ELASTIC_API = axios.create({
    baseURL: 'https://admin:admin@52.48.56.124:9200',
    httpsAgent: new https.Agent({
        rejectUnauthorized: false
    })
});
BAR.start(TOTAL, 0);


//retryErrors().then();


run(TOTAL, CONCURRENCY_STEP, STEP_BY, REQUEST_LIMIT, CONCURRENCY_REQUEST_FOR_EACH_STEP).then(r => {
    check("ALL")
    retryErrors().then();
})


async function retryErrors() {
    const data = (readFileSync(logPath) + "")
        .split("\r\n")
        .map(x => x.split(";"))
        .filter(x => !isNaN(parseInt(x[1])))
        .filter(x[0] === "KO")
        .map(x => ({FROM: parseInt(x[1]), TO: parseInt(x[2])}))
        .sort((a, b) => a.FROM - b.FROM)

    const headers = getHeaders();
    return Promise.all(data.map(x => get(headers, REQUEST_LIMIT, x.FROM)))

}


function check(lastPid) {
    const data = (readFileSync(logPath) + "")
        .split("\r\n")
        .map(x => x.split(";"))
        .filter(x => !isNaN(parseInt(x[1])))
        .map(x => ({FROM: parseInt(x[1]), TO: parseInt(x[2])}))
        .sort((a, b) => a.FROM - b.FROM)
        .reduce((acc, curr, index, self) => {
            if (index === 0) return true;
            if (curr.FROM !== self[index - 1].TO) throw new Error("" + curr.FROM)
            return acc && curr.FROM === self[index - 1].TO
        }, true)
    log(lastPid + (data ? 'OK' : 'KO'));
}


async function run(TOTAL, CONCURRENCY_STEP, STEP_BY, REQUEST_LIMIT, CONCURRENCY_REQUEST_FOR_EACH_STEP) {

    const stepCount = TOTAL / (STEP_BY);
    log(stepCount);
    for (let i = 0; i < stepCount; i = i + CONCURRENCY_STEP) {
        log("FROM", i, i + CONCURRENCY_STEP)
        await run_step(i, CONCURRENCY_STEP, STEP_BY, REQUEST_LIMIT, CONCURRENCY_REQUEST_FOR_EACH_STEP)
    }

    BAR.stop();

}


async function run_step(from, CONCURRENCY_STEP, STEP_BY, REQUEST_LIMIT, CONCURRENCY_REQUEST_FOR_EACH_STEP) {
    const START_RUN_STEP = new Date().getTime();
    console.log("RUN STEP", {from, CONCURRENCY_STEP, STEP_BY})
    await Promise.all(
        (Array.from(Array(CONCURRENCY_STEP).keys()).map(index =>
                start(
                    index,
                    (from + index) * STEP_BY,
                    (from + index + 1) * STEP_BY,
                    REQUEST_LIMIT,
                    CONCURRENCY_REQUEST_FOR_EACH_STEP
                ))
        )
    ).then(x => {

        log(
            "END ",
            "FROM",
            from,
            "TO",
            (from + CONCURRENCY_STEP) * STEP_BY,
            new Date().getTime() - START_ALL_P, "(FROM ORIGIN)",
            new Date().getTime() - START_RUN_STEP, "(THIS STEP)",)
    });

    check("0 - " + (from + CONCURRENCY_STEP) * STEP_BY);
    return null
}


//start(10000, 15000).then(x=> log("-------------------- END 1 ",  new Date().getTime() - START_ALL_P ));
//start(15000, 20000).then(x=> log("-------------------- END 1 ",  new Date().getTime() - START_ALL_P ));

//start(15000, 30000).then(x=> log("-------------------- END 2 ",  new Date().getTime() - START_ALL_P ));
//start(20000, 30000).then(x=> log("-------------------- END 3 ",  new Date().getTime() - START_ALL_P ));


function getTargetBaseUrl(host, port) {
    return host + (port ? ':' + port : '');
}


async function start(step, from, to, REQUEST_LIMIT, CONCURRENCY_REQUEST_FOR_EACH_STEP) {
    //return new Promise(resolve=> setTimeout(resolve(),1000))
    console.log("START", {step, from, to, REQUEST_LIMIT})

    const pid = [new Date().getTime(), ('00000000000' + from).slice(-6), ('00000000000' + to).slice(-6)].join("-");

    let loopSize = REQUEST_LIMIT * CONCURRENCY_REQUEST_FOR_EACH_STEP;
    let index = from;
    let nowFrom = null;
    let nowTo = null;

    const host = process.env.DIRECTUS_HOST
    const port = process.env.DIRECTUS_PORT
    const indexName = process.env.ELASTIC_INDEX


    const headers = getHeaders();

    while (true) {
        nowFrom = (index);
        nowTo = (index + loopSize);

        if (nowTo > to) {
            return
        }

        const now = new Date().getTime();
        const pagePromise = []
        for (let i = 0; i < loopSize / REQUEST_LIMIT; i++) {
            pagePromise.push(get(headers, REQUEST_LIMIT, index))
            index = index + REQUEST_LIMIT;
        }

        const data = (await Promise.all(pagePromise)
            .then(r => {
                const data = r.map(x => x.data).flat().map(mapInDoc);
                r
                    //.filter(x => x.start === 'KO')
                    .forEach(x => {

                        if (x.status === "KO") {
                            log("ERROR ", "FROM", x.from, "TO", x.to)
                        }
                        appendFile(logPath, [x.status, x.from, x.to].join(";") + "\r\n", () => {
                        });

                    })

                return data;
            }));

        const afterDB = new Date().getTime();
        const dataEs = data.map(x => ([{"index": {"_index": indexName}}, x])).flat()

        await ES_CLIENT.bulk({body: dataEs}).catch(e => {
                console.log(e);
                throw e
            }
        )
        /*
         await ELASTIC_API.post('_bulk', dataEsFake).catch(e => {
         console.log(e.toJSON())
         throw e;
         })
         */
        const after = new Date().getTime();

        log(pid, "RUN DB " + (afterDB - now) / 1000, "RUN ALL " + (after - now) / 1000, "START=" + nowFrom, "END=" + nowTo, "SIZE=" + data.length)

        BAR.increment(loopSize);

        if (data.length < REQUEST_LIMIT) {
            return;
        }
        //log(JSON.stringify(dataEs, null, 3))
        //await elastic.post('/test_001/_bulk', dataEs)

    }


}

function mapInDoc(directus_item) {
    const doc = {
        id: directus_item.id,
        license_windows: directus_item.license_windows_id,
        combination: directus_item.right_combinations_id,
        consumption: directus_item.license_windows_id.consumption,
        contract: {id: directus_item.right_combinations_id.set.contract_item.contracts_id},
        catalog: {id: directus_item.right_combinations_id.set.contract_item.catalogs_id},
        set: directus_item.right_combinations_id.set,
    }

    doc.license_windows.duration = Math.ceil(Math.abs(new Date(doc.license_windows.end_date) - new Date(doc.license_windows.start_date)) / (1000 * 60 * 60 * 24));
    doc.combination.values = doc.combination.dimensions.map(x => x.values.map(x => x.matrix_value)).flat()

    if (doc.consumption) {
        doc.consumption.scheduled_planned = doc.consumption.scheduled_runs;
        doc.consumption.transmitted_published = doc.consumption.transmitted_runs;
        doc.consumption.gross_available = doc.consumption.available_runs
        doc.consumption.net_available = doc.consumption.available_runs

        delete doc.consumption.scheduled_runs;
        delete doc.consumption.transmitted_runs;

    }

    delete doc.combination.dimensions
    delete doc.combination.set
    delete doc.set.contract_item
    delete doc.license_windows.set
    delete doc.license_windows.consumption


    return doc


}

class Retry extends Error {
}

function get(headers, pageSize, from = 0) {
    const collectionName = 'right_combinations_license_windows';
    const fields = [
        "id",
        "right_combinations_id.id",
        "right_combinations_id.exclusive",
        "right_combinations_id.dimensions.values.matrix_value",
        "right_combinations_id.set.id",
        "right_combinations_id.set.contract_item.id",
        "right_combinations_id.set.contract_item.catalogs_id",
        "right_combinations_id.set.contract_item.contracts_id",


        /*
         "right_combinations_id.set.contract_item.contracts_id.id",
         "right_combinations_id.set.contract_item.contracts_id.status",
         "right_combinations_id.set.contract_item.contracts_id.licensor",
         "right_combinations_id.set.contract_item.contracts_id.licensee",
         "right_combinations_id.set.contract_item.contracts_id.type",
         "right_combinations_id.set.contract_item.contracts_id.sub_type",


         "right_combinations_id.set.contract_item.catalogs_id.id",
         "right_combinations_id.set.contract_item.catalogs_id.type",
         "right_combinations_id.set.contract_item.catalogs_id.category",
         "right_combinations_id.set.contract_item.catalogs_id.genres",
         "right_combinations_id.set.contract_item.catalogs_id.release_year",
         "right_combinations_id.set.contract_item.catalogs_id.duration",
         */

        "license_windows_id.id",
        "license_windows_id.type",
        "license_windows_id.start_date",
        "license_windows_id.end_date",

        "license_windows_id.consumption.id",
        "license_windows_id.consumption.exclusive",
        "license_windows_id.consumption.contractual_runs",
        "license_windows_id.consumption.scheduled_runs",
        "license_windows_id.consumption.transmitted_runs",
        "license_windows_id.consumption.sold_runs",
        "license_windows_id.consumption.available_runs",
        "license_windows_id.consumption.unit",

    ];
    const limit = pageSize;
    const offset = from;
    const url = '/items/' + collectionName + "?limit=" + limit + "&offset=" + from + "&fields=" + fields.join(',') + "&sort=id";

    return new Promise((resolve, reject) => {
        DIRECTUS_API.get(url, {headers})
            .then(r => {

                    resolve({
                        status: 'OK',
                        data: r.data.data,
                        from: offset,
                        to: offset + limit,
                    })

                }
            )
            .catch(e => {
                console.log(e);
                resolve({
                    status: 'KO',
                    data: [],
                    from: offset,
                    to: offset + limit,
                })
            })
    })

}


function getHeaders() {
    const token = process.env.DIRECTUS_TOKEN;

    let ret = {};
    ret['authorization'] = "Bearer " + token;
    ret['content-type'] = 'application/json';

    //ret['cache-control'] = 'no-cache';

    return ret;
}

function log(...x) {
    const message = x.join("\t")
    appendFile(logCliPath, message + "\r\n", () => {
    })
}