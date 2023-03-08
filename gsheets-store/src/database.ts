import assert from 'assert'
import {GlobalOptions, sheets, sheets_v4} from '@googleapis/sheets'
import {Table, TableWriter} from './table'
import {createHash} from 'crypto'

type TableMap = Record<string, Table<any>>

export interface DatabaseOptions<T extends TableMap> {
    tables: T

    spreadsheetId: string

    options: GlobalOptions
}

type Chunk<T extends TableMap> = {
    [k in keyof T]: TableWriter<T[k] extends Table<infer R> ? R : never>
}

type StoreWriter<W extends TableWriter<any>> = Pick<W, 'insert' | 'insertMany'>

export type Store<T extends TableMap> = Readonly<{
    [k in keyof T]: StoreWriter<Chunk<T>[k]>
}>

interface StoreConstructor<T extends TableMap> {
    new (chunk: () => Chunk<T>): Store<T>
}

export class Database<T extends TableMap> {
    protected tables: T

    protected spreadsheetId: string

    protected sheetsClient: sheets_v4.Sheets
    protected lastCommitted = -1

    protected StoreConstructor: StoreConstructor<T>

    constructor(options: DatabaseOptions<T>) {
        this.tables = options.tables
        this.sheetsClient = sheets({...options.options, version: 'v4'})
        this.spreadsheetId = options.spreadsheetId

        class Store {
            constructor(protected chunk: () => Chunk<T>) {}
        }
        for (let name in this.tables) {
            Object.defineProperty(Store.prototype, name, {
                get(this: Store) {
                    return this.chunk()[name]
                },
            })
        }
        this.StoreConstructor = Store as any
    }

    async connect(): Promise<number> {
        let {data: spreadsheet} = await this.sheetsClient.spreadsheets.get({spreadsheetId: this.spreadsheetId})

        let statusSheet = spreadsheet.sheets?.find((s) => s.properties?.title === 'squid_status')
        if (statusSheet == null) {
            await this.sheetsClient.spreadsheets.batchUpdate({
                spreadsheetId: this.spreadsheetId,
                requestBody: {
                    requests: [
                        {
                            addSheet: {
                                properties: {
                                    title: 'squid_status',
                                    gridProperties: {rowCount: 1, columnCount: 1},
                                },
                            },
                        },
                    ],
                },
            })
            this.lastCommitted = -1
        } else {
            let {data} = await this.sheetsClient.spreadsheets.values.get({
                spreadsheetId: this.spreadsheetId,
                range: 'squid_status!R1C1',
            })
            this.lastCommitted = Number(data.values?.[0]?.[0] ?? -1)
        }

        await this.migrate()

        return this.lastCommitted
    }

    private async migrate() {
        let {data: spreadsheet} = await this.sheetsClient.spreadsheets.get({spreadsheetId: this.spreadsheetId})
        let newTables = Object.values(this.tables).filter(
            (t) => spreadsheet.sheets?.findIndex((s) => s.properties?.title == t.name) === -1
        )
        if (newTables.length > 0) {
            await this.sheetsClient.spreadsheets.batchUpdate({
                spreadsheetId: this.spreadsheetId,
                requestBody: {
                    requests: newTables
                        .map((t) => {
                            let sheetId = createHash('md5').update(t.name).digest().readUint16LE()
                            return [
                                {
                                    addSheet: {
                                        properties: {
                                            sheetId,
                                            title: t.name,
                                            gridProperties: {rowCount: 1, columnCount: t.columns.length},
                                        },
                                    },
                                },
                                {
                                    updateCells: {
                                        range: {
                                            sheetId,
                                            startColumnIndex: 0,
                                            endColumnIndex: t.columns.length,
                                            startRowIndex: 0,
                                            endRowIndex: 1,
                                        },
                                        rows: [
                                            {
                                                values: t.columns.map((c) => ({
                                                    userEnteredValue: {stringValue: c.name},
                                                    userEnteredFormat: {numberFormat: {type: c.data.type.formatType}},
                                                })),
                                            },
                                        ],
                                        fields: 'userEnteredValue(stringValue),userEnteredFormat(numberFormat)',
                                    },
                                },
                            ]
                        })
                        .flat(),
                },
            })
        }
    }

    async close(): Promise<void> {
        this.lastCommitted = -1
    }

    async transact(from: number, to: number, cb: (store: Store<T>) => Promise<void>): Promise<void> {
        let open = true

        let chunk = this.createChunk()
        let store = new this.StoreConstructor(() => {
            assert(open, `Transaction was already closed`)
            return chunk
        })

        try {
            await cb(store)

            let tableAliases = Object.keys(this.tables)

            let newRowIndecies = await Promise.all(tableAliases.map((a) => this.getNextRowIndex(this.tables[a].name)))

            let requests: sheets_v4.Schema$ValueRange[] = []
            for (let i = 0; i < tableAliases.length; i++) {
                let table = this.tables[tableAliases[i]]
                requests.push({
                    range: `'${table.name}'!R${newRowIndecies[i] + 1}C1:C${table.columns.length}`,
                    values: chunk[tableAliases[i]].flush(),
                })
            }
            requests.push({
                range: `squid_status!R1C1`,
                values: [[to]],
            })

            await this.sheetsClient.spreadsheets.values.batchUpdate({
                spreadsheetId: this.spreadsheetId,
                requestBody: {
                    data: requests,
                    valueInputOption: 'USER_ENTERED',
                    includeValuesInResponse: false,
                },
            })

            this.lastCommitted = to
        } catch (e: any) {
            open = false
            throw e
        }

        open = false
    }

    async advance(height: number): Promise<void> {
        if (this.lastCommitted == height) return

        await this.sheetsClient.spreadsheets.values.update({
            spreadsheetId: this.spreadsheetId,
            range: `squid_status!R1C1`,
            requestBody: {
                values: [[height]],
            },
            valueInputOption: 'USER_ENTERED',
        })
    }

    private async getNextRowIndex(sheet: string): Promise<number> {
        let {data} = await this.sheetsClient.spreadsheets.values.append({
            spreadsheetId: '1g6aHhV7-EsB6SRmM9KIQq9y_Mm4ZVfMw_5aK79NvDXg',
            range: `'${sheet}'`,
            valueInputOption: 'RAW',
            requestBody: {
                values: [['']],
            },
        })
        assert(data.tableRange != null)
        let index = /^.*![A-Z]+\d+:[A-Z]+(\d+)$/.exec(data.tableRange)?.[1]
        assert(index != null)
        return Number(index)
    }

    private createChunk(): Chunk<T> {
        let chunk: Chunk<T> = {} as any
        for (let name in this.tables) {
            chunk[name] = this.tables[name].createWriter()
        }
        return chunk
    }
}
