const fs = require('fs');
const util = require('util');
const config = require('./config');

class CsvDB {
    /**
     * opts {} object to override default config.
     */
    constructor (file, fields = null, opts = {}) {
        this.file = file;
        this.fields = fields;
        if( opts.delimiter ) config.delimiter = opts.delimiter;
        if( opts.hasOwnProperty('header') ) config.header = opts.header;
    }

    /**
     * Read CSV file 
     */
    getFileContent() {
        return new Promise((resolve, reject) => {
            fs.readFile(this.file, 'utf-8', (err, data) => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve(data);
            });
        });
    }

    /**
     * transform CSV data into array of rows, cols
     */
    transform(input) {
        if (input === '') {
            return [];
        }

        const lines = input.split(config.lineSeparator);
        let fields;

        if (!config.header && this.fields === null) 
            throw {name : "InvalidConfigError", message : "Fields should be specified or in the file header"}; ;

        if (config.header) {
            fields = lines
                .shift()
                .split(config.delimiter)
                .map(field => field.trim());
            if (fields[fields.length -1] === '') {
                fields.pop();
            }
        } else {
            fields = this.fields;
        }
        const result = [];
        if (Array.isArray(lines) && lines.length > 0) {
            for (let i = 0; i < lines.length; i++) {
                result[i] = {};
                let cols = lines[i].split(config.delimiter);

                for (let j = 0; j < fields.length; j++) {
                    result[i][fields[j]] = cols[j];
                }
            }
        }
        return result;
    }

    /**
     * Read cRud operation
     */
    get(id) {
        return new Promise((resolve, reject) => {
            this.getFileContent().then((data) => {

                let result = this.transform(data);

                if (id) {
                    result = result.filter((row) => {
                        if( typeof id === 'number' || typeof id === 'undefined' ) {
                            return row['id'] == id;
                        } else if( typeof id === 'object' ) {
                            var ret = true;
                            Object.keys( id ).forEach( (key) => { 
                                if( row[key] != id[key]) ret = false; 
                            } );
                            return ret;
                        }
                    }).pop();
                }

                resolve(result);
            }, err => reject(err));
        });
    }

    getNextId() {
        return new Promise((resolve, reject) => {
            this.get().then(data => {
                if (Array.isArray(data) && data.length === 0) {
                    resolve(1);
                    return;
                }

                const lastIndex = data.reduce((prev, curr) => {
                    const itemId = parseInt(curr['id'], 10);
                    return (prev < itemId) ?
                        itemId :
                        prev;
                }, 0);

                resolve(lastIndex + 1);
            }, err => deferred.reject(err));
        });
    }

    /**
     * Create: Crud operation
     */
    insert(newData) {
        return new Promise((resolve, reject) => {
            this.get().then(data => {
                this.getNextId().then((id) => {
                    newData.id = id;
                    data.push(newData);
                    return this.write(this.flatten(data));
                }, () => reject());
            }, () => reject());
        });
    }

    /**
     * Prepare the data for ouput to CSV file
     */
    flatten(input) {
        const result = [];
        let row = [];

        if (Array.isArray(input) && input.length > 0) {

            if (config.header) {
                let fields = Object.keys(input[0]);
                result.push(fields.join(config.delimiter) + config.delimiter);
            }
            for (let i = 0; i < input.length; i++) {
                row = [];
                for (let j = 0; j < fields.length; j++) {
                    row.push(input[i][fields[j]]);
                }
                result.push(row.join(config.delimiter) + config.delimiter);
            }
        }
        return result.join(config.lineSeparator);
    }

    /**
     * Update: crUd operation
     */
    update(data, id={}) {
        return new Promise((resolve, reject) => { 
            this.get().then((existingContent) => {
                if (!Array.isArray(data)) {
                    data = [data];
                }
                for (let i = 0; i < data.length; i++) {
                    existingContent = existingContent.map(item => {
                        if( typeof id === 'number' ) {
                            if (item.id === data[i].id.toString) {
                                return Object.assign(item, data[i]);
                            }
                        } else if( typeof id === 'object') {
                            var match = true;
                            Object.keys( id ).forEach( (key) => { 
                                if( item[key] != id[key]) match = false; 
                            } );
                            if( match ) {
                                return Object.assign( item, data[i] );
                            }
                        }
                        return item;
                    });
                }

                return this.write(this.flatten(existingContent));
            }, () => reject()).then(() => resolve(), err => reject(err));
        });
    }

    /**
     * Write 'flattened' data to file
     */
    write(data) {
        return new Promise((resolve, reject) => {
            fs.writeFile(this.file, data, err => {
                if (err) {
                    reject(err);
                    return;
                }
                resolve();
            });

        });
    }

    /**
     * Delete: cruD operation
     */
    delete(id) {
        return new Promise((resolve, reject) => {
            this.get().then(data => {
                for (let i = 0; i < data.length; i++) {
                    if (data[i].id == id) {
                        data.splice(i, 1)
                        break;
                    }
                }
                return this.write(this.flatten(data));
            }, () => reject()).then(()  => resolve(), err => reject(err));
        });
    }
}

module.exports = CsvDB;
