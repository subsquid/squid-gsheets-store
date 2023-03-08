import assert from 'assert'
import {Type} from './table'

export function String(): Type<string> {
    return {
        formatType: 'TEXT',
        serialize(value: string) {
            assert(typeof value === 'string')
            return value
        },
    }
}

export function Numeric(): Type<number | bigint> {
    return {
        formatType: 'TEXT',
        serialize(value) {
            switch (typeof value) {
                case 'bigint':
                    return value.toString()
                case 'number':
                    return value
                default:
                    throw new Error()
            }
        },
    }
}

export function DateTime(): Type<Date> {
    return {
        formatType: 'DATE_TIME',
        serialize(value) {
            return (value.valueOf() / 86400000) + 25569
        },
    }
}

export function Boolean(): Type<boolean> {
    return {
        formatType: 'TEXT',
        serialize(value: boolean) {
            assert(typeof value === 'boolean', 'Invalid boolean')
            return value
        },
    }
}
