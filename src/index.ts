import dayjs from "dayjs";
import { Database, Statement } from "sqlite3";
import sqlite3 from "sqlite3";
import fs from "fs";
import utc from 'dayjs/plugin/utc'
import timezone from 'dayjs/plugin/timezone'

dayjs.extend(utc)
dayjs.extend(timezone)

function runQuery(dbase: Database, sql: string, params: Array<void>) {
    return new Promise<any>((resolve, reject) => {
        return dbase.all(sql, params, (err: any, res: any) => {
            if (err) {
                reject(err.message);
            }
            resolve(res);
        });
    });
}

function execStatement(stmt: Statement, params: (string | number)[]): Promise<void> {
    return new Promise<void>((resolve, reject) => {
        stmt.run(params, (err: any, res: any) => {
            if (err) {
                reject(err.message);
            }
            resolve(res);
        });
    });
}

async function finalizeAndCommit(db: Database, stmt: Statement): Promise<void> {
    return new Promise<void>(async (resolve, reject) => {
        try {
            stmt.finalize();
            await runQuery(db, "COMMIT", []);
            resolve();
        } catch (err) {
            reject(err);
        }
    })
}

async function createNewDb(dbFileName: string): Promise<[Database, Statement]> {
    return new Promise<[Database, Statement]>(async (resolve, reject) => {
        try {
            let db = new Database(dbFileName, sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE);
            await runQuery(db, "BEGIN", []);
            await runQuery(db, `CREATE TABLE "Measurements" ("id" INTEGER NOT NULL,"channel" INTEGER,"measured_value" REAL,"recorded_time" INTEGER, PRIMARY KEY("id" AUTOINCREMENT))`, []);
            let stmt = db.prepare("INSERT INTO Measurements (channel, measured_value, recorded_time) VALUES (?, ?, ?)");
            return resolve([db, stmt]);
        } catch (err) {
            reject(err);
        }
    })
}

async function insertMeasurements(db: Database, stmt: Statement, measuredValue: number, hourlyIterator: dayjs.Dayjs): Promise<void> {
    let promises: Promise<void>[] = [];
    for (let channel = 1; channel <= 12; channel++) {
        promises.push(execStatement(stmt, [channel, measuredValue, hourlyIterator.unix()]));
    }
    await Promise.all(promises);
}

async function generateMeasurements(year: number, generatedYears: number, timeZone: string) {

    let hourlyIterator = dayjs.tz(year + "01-01"); //, timeZone
    let endOfYear = dayjs.tz(year + generatedYears + "01-01", timeZone);
    let measuredValue = 0;
    let db: Database | null = null;
    let stmt: Statement | null = null;
    while (hourlyIterator.isBefore(endOfYear) || hourlyIterator.isSame(endOfYear)) {
        if ((hourlyIterator.get("date") == 1) && (hourlyIterator.get("hour") == 0)) {
            if (db && stmt) {
                await finalizeAndCommit(db, stmt)
            }
            let dbFileName = hourlyIterator.format("YYYY-MM") + '-monthly.sqlite';
            if (fs.existsSync(dbFileName)) {
                fs.rmSync(dbFileName)
            }
            [db, stmt] = await createNewDb(dbFileName);
            console.log(dayjs().format(), `DB file '${dbFileName}' created.`);
        }

        if (db && stmt) {
            try {
                await insertMeasurements(db, stmt, measuredValue, hourlyIterator);
            } catch (err) {
                console.error(err);
            }
        }
        measuredValue += 100;
        hourlyIterator = hourlyIterator.add(1, "hour");
    }

    if (db && stmt) {
        try {
            await finalizeAndCommit(db, stmt);
        } catch (err) {
            console.error(err);
        }
    }
}

generateMeasurements(2023, 1, "America/Los_Angeles").catch((reason) => {
    console.error(reason);
}).then(() => console.log("Factoring finished.")); //"Europe/Budapest"