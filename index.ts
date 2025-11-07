import Koa from 'koa';
import Router from 'koa-router';
import bodyParser from 'koa-bodyparser';
import crypto from 'crypto';
import koaBody from 'koa-body';
import XLSX from "xlsx";
import fs from "fs";
import path from "path";
import { pg } from './db'
import knex from 'knex';
const app = new Koa();
app.use(bodyParser())
// app.use(koaBody({ multipart: true }));
const r = new Router()

const md5sum = (content: string) => {
    const md5 = crypto.createHash('md5');//TODO:

    return md5.update(content).digest("hex")
}
interface TData {
    tn: string;
    data: any[];
}
r.get("/", async (ctx) => {
    const param = ctx.request.body
    console.log(param);
    ctx.response.body = { "ok": 1 }
})

r.post("/upload", koaBody({ multipart: true }), async (ctx) => {
    const param = ctx.request.body as any
    const f = ctx.request.files
    const h = ctx.request.headers
    let uid = param.uid
    let cid = param.cid
    if (!uid) uid = 'tmp'
    if (!cid) uid = 'tmp'
    // console.log(h);
    // console.log(param);
    if (!h.auth || "6kFM5Cmj7cwAwf3X2ucm" !== h.auth) {
        ctx.response.status = 401
        return ctx.response.body = { "ok": 0 }
    }
    if (f) {
        var ff = f.file
        if (ff instanceof Array) {
            console.log("adsasd");
            console.log(ff.pop()?.filepath);
            //TODO: 
        } else {
            // console.log(ff);

            console.log(ff.filepath);
            const workbook = XLSX.readFile(ff.filepath);
            const td: TData[] = []
            workbook.SheetNames.forEach((sheetName) => {
                const worksheet = workbook.Sheets[sheetName];
                const jsonData = XLSX.utils.sheet_to_json(worksheet);
                // console.log(jsonData);
                const tn = `${sheetName}_${md5sum(`${param.uid}_${param.cid}`).substring(1, 7)}`
                td.push({ tn, data: jsonData })

            });
            const rd = await produceData(td);
            return ctx.response.body = { "ok": 1, "rez": rd }
        }
    }




    return ctx.response.body = { "ok": 1 }
})

r.post("/process/sql", async (ctx) => {
    const param = ctx.request.body as any
    const h = ctx.request.headers
    let uid = param.uid
    let cid = param.cid
    let sql = param.sql
    if (!uid) uid = 'tmp'
    if (!cid) uid = 'tmp'
    // console.log(h);
    console.log(param);
    if (!h.auth || "6kFM5Cmj7cwAwf3X2ucm" !== h.auth) {//TODO: 
        ctx.response.status = 401
        return ctx.response.body = { "ok": 0 }
    }
    if (!sql || typeof (sql) != "string") {
        ctx.response.status = 403
        return ctx.response.body = { "ok": 0 }
    }
    if (sql.startsWith("```")) {
        sql = sql.replaceAll(/```.*/g, "").trim()
    }

    try {
        const rez = await pg.raw(sql);
        // console.log(rez);

        return ctx.response.body = { "ok": 1, "rez": rez.rows }
    } catch (err) {
        // let sql2 = sql.match(/SELECT.*;/im)
        console.log(err);

        return ctx.response.body = { "ok": 2, "rez": "SQL执行失败，请重新提问" }
    }

})



const produceData = async (data: TData[]) => {
    let rez: string = ""
    for (const i of data) {
        if (!(await pg.schema.hasTable(i.tn)))
            await createTable(i.tn, i.data[0])
        await pg.batchInsert(i.tn, i.data, 128)
        const s = await getTableSchema(i.tn);
        const d = await getTop10Data(i.tn)
        rez += `\`\`\`${i.tn}.sql\n${s}\`\`\`\n\n 以下是该表的前10条数据\`\`\`${i.tn}_top_10_data.json\n${JSON.stringify(d)}\n\`\`\`\n\n`
    }
    return rez;
}


const createTable = async (tn: string, schema: any) => {
    await pg.schema.createTable(tn, table => {
        // table.increments()
        for (const i of Object.keys(schema)) {
            // console.log(i);
            // if(i=="ID"){
            //     table.text("客户ID").index();
            //     continue;
            // }
            if (i.includes("日期")) {
                table.date(i).index();
                continue;
            }
            if (i.match(/时..间/)) {
                table.time(i).index();
                continue;
            }
            switch (typeof (schema[i])) {
                case 'string': table.text(i).index(); break;
                case 'number': table.double(i).index(); break;
                case 'bigint': table.bigint(i).index(); break;
                case 'boolean': table.boolean(i); break;
            }

        }
    })

}

const getTableSchema = async (tn: string) => {
    const r = await pg.raw(`SELECT
    'CREATE TABLE ' || relname || E'\n(\n' ||
    array_to_string(
            array_agg(
                    '    ' || column_name || ' ' ||  type || ' '|| not_null
            )
        , E',\n'
    ) || E'\n);\n'
from
    (
        SELECT
            c.relname, a.attname AS column_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) as type,
            case
                when a.attnotnull
                    then 'NOT NULL'
                else 'NULL'
                END as not_null
        FROM pg_class c,
             pg_attribute a,
             pg_type t
        WHERE c.relname = '${tn}'
          AND a.attnum > 0
          AND a.attrelid = c.oid
          AND a.atttypid = t.oid
        ORDER BY a.attnum
    ) as tabledefinition
group by relname
;`)
    return r.rows[0]["?column?"]
}

const getTop10Data = async (tn: string) => {
    return pg(tn).select().limit(10);
}





app.use(r.routes());
app.use(r.allowedMethods())
app.listen(50020);