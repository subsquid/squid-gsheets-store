type Primitive = string | number | boolean

export interface Type<T> {
    formatType: string,
    serialize(value: T): Primitive
}

export interface ColumnOptions {
    nullable?: boolean
}

export interface ColumnData<
    T extends Type<any> = Type<any>,
    O extends Required<ColumnOptions> = Required<ColumnOptions>
> {
    type: T
    options: O
}

export interface Column {
    name: string
    data: ColumnData
}

export interface TableSchema {
    [column: string]: ColumnData
}

type NullableColumns<T extends Record<string, ColumnData>> = {
    [F in keyof T]: T[F] extends ColumnData<any, infer R> ? (R extends {nullable: true} ? F : never) : never
}[keyof T]

type ColumnsToTypes<T extends Record<string, ColumnData>> = Simplify<
    {
        [F in Exclude<keyof T, NullableColumns<T>>]: T[F] extends ColumnData<Type<infer R>> ? R : never
    } & {
        [F in Extract<keyof T, NullableColumns<T>>]?: T[F] extends ColumnData<Type<infer R>>
            ? R | null | undefined
            : never
    }
>

export class Table<T extends ColumnsToTypes<S>, S extends TableSchema = any> {
    readonly columns: ReadonlyArray<Column>
    constructor(readonly name: string, schema: S) {
        let columns: Column[] = []
        for (let column in schema) {
            columns.push({
                name: column,
                data: schema[column],
            })
        }
        this.columns = columns
    }

    createWriter(): TableWriter<T> {
        return new TableWriter(this.columns)
    }
}

export class TableWriter<T extends Record<string, any>> {
    private records: T[] = []

    constructor(private columns: ReadonlyArray<Column>) {}

    flush() {
        let res: (Primitive | null)[][] = []

        for (let record of this.records) {
            let values: (Primitive | null)[] = []
            for (let column of this.columns) {
                let value = record[column.name]
                values.push(value == null ? null : column.data.type.serialize(value))
            }

            res.push(values)
        }
        return res
    }

    insert(record: T): this {
        this.records.push(record)
        return this
    }

    insertMany(records: T[]): this {
        this.records.push(...records)
        return this
    }
}

export type TableRecord<T extends Table<any>> = T extends Table<infer R> ? R : never

type Simplify<T> = {
    [K in keyof T]: T[K]
} & {}

export function Column<T extends Type<any>>(type: T): ColumnData<T>
export function Column<T extends Type<any>, O extends ColumnOptions>(
    type: T,
    options?: O
): ColumnData<T, O & Required<ColumnOptions>>
export function Column<T extends Type<any>>(type: T, options?: ColumnOptions) {
    return {
        type,
        options: {nullable: false, ...options},
    }
}
