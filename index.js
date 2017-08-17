const aws = require('aws-sdk');

class ServerlessPlugin {
  constructor(serverless, options) {
    this.serverless = serverless;
    this.options = options;

    this.commands = {
      deploy: {
        lifecycleEvents: [
          'resources',
        ]
      },
    };

    this.hooks = {
      'after:deploy:resources': this.run.bind(this)
    };
  }

  run() {
    return new Promise((resolve, reject) => {
      aws.config.update({region:this.serverless.service.provider.region})
      const athena = new aws.Athena()
      const params = this.serverless.service.custom.athena
      const service = this.serverless.service.service
      const stage = this.serverless.service.provider.stage
      const dbName = `${service}-${stage}`.split('-').join('_')

      console.log(dbName, service, stage)

      const createDbParams = {
        QueryString: `
          CREATE DATABASE IF NOT EXISTS ${dbName}
          LOCATION 's3://${service}-${stage}-data/';`,
        ResultConfiguration: {
          OutputLocation: `s3://${service}-${stage}-results/output/`
        },
      }

      this.serverless.cli.log(`Trying to deploy athena`)

      athena.startQueryExecution(createDbParams).promise().then((res) => {
        this.serverless.cli.log('Successfully deployed athena database...')
        const tablePromises = []


        // console.log(this.serverless.service.custom.athena.tables)

        for(const table of this.serverless.service.custom.athena.tables) {
          const dropTableParams = {
            QueryString: `
              DROP TABLE IF EXISTS ${table.name}`,
            ResultConfiguration: {
              OutputLocation: `s3://${service}-${stage}-results/output/`
            },
            QueryExecutionContext: {
              Database: dbName
            }
          }
          tablePromises.push(athena.startQueryExecution(dropTableParams).promise().then(() => {
            const columns = table.columns

            let columnString = 'id string,created string,updated string,'
            for(const column of columns) {
              columnString += `${column},`
            }
            columnString = columnString.slice(0,-1)

            const createTableParams = {
              QueryString: `
                CREATE EXTERNAL TABLE ${table.name} (${columnString})
                PARTITIONED BY (year string, month string, day string, hour string)
                LOCATION 's3://${service}-${stage}-data/${table.name}/';`,
              ResultConfiguration: {
                OutputLocation: `s3://${service}-${stage}-results/output/`
              },
              QueryExecutionContext: {
                Database: dbName
              }
            }
            athena.startQueryExecution(createTableParams).promise()
          }).catch((err) => {
            reject(err)
          }))
        }



        Promise.all(tablePromises).then(() => {
          resolve()
          this.serverless.cli.log('Successfully deployed athena tables...')
        }).catch((err) => {
          reject(err)
          this.serverless.cli.log('Error deploying athena tables...', err)
        })
      }).catch((err) => {
        console.log('error!', err)
        reject(err)
      })
    })
  }
}

module.exports = ServerlessPlugin;
