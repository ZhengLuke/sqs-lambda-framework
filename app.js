'use strict';

const AWS = require('aws-sdk');
const async = require('async');
const mysql = require('mysql');
const config = require('./config.json');
const parseString = require('xml2js').parseString;

const dbs_config = config.rdsConnection;
const emailFrom = config.emailFrom;
const emailTo = config.emailReceipient.to;
const emailCc = config.emailReceipient.cc;

let TASK_QUEUE_URL = config.taskQueueUrl;
let AWS_REGION = config.AWSRegion;
let sqs = new AWS.SQS({region: AWS_REGION});
let ses = new AWS.SES({region: AWS_REGION});


function handleSqsMsg(trackingList, callback){
    queueMessageNum((err, res) => {
        if (err){
            callback(err)
        } else{
            if (res > 0){
                let batchNum = Math.ceil(res / 10);
                let mq = async.queue(processMsgBatch, 6);
                let dq = async.queue(recordMsg, 6);

                for (let i = 0; i < batchNum; i++) {
                    mq.push(null, function(err, res){
                        if (err) callback(err)
                        else {res.map((r) => {
                            dq.push(r, (err, res) => {
                          // do something
                        })
                        }
                    })
                }

                mq.drain = function(){
                    if (dq.empty){
                        callback(null, 'done')
                    }
                };

                dq.drain = function(){
                    if (mq.empty){
                        callback(null, 'done')
                    }
                }
            }else {callback(null, 'No message available')}
        }
    })
}


function recordMsg(msgInfo, callback){
    const connection = mysql.createConnection({
        host: dbs_config.host
        , user: dbs_config.usr
        , password: dbs_config.pwd
        , database: dbs_config.database
    });

    connection.connect((err) => {
        // do something
            })
        }
    });
}


function queueMessageNum(callback) {
    let params = {
        QueueUrl: TASK_QUEUE_URL
        , AttributeNames: ['MaximumMessageSize', 'ApproximateNumberOfMessages']
    };
    sqs.getQueueAttributes(params, function(err, data) {
        if (err) {callback(err)}
        else {
            let approximateNumberOfMessages = data['Attributes']['ApproximateNumberOfMessages'];
            if (approximateNumberOfMessages > 0) {
                callback(null, approximateNumberOfMessages)
            } else {
                callback(null, 0)
            }
        }
    });
}


function processMsgBatch(trig, callback) {
    let params = {
        QueueUrl: TASK_QUEUE_URL,
        MaxNumberOfMessages: 10
    };
    sqs.receiveMessage(params, function(err, data) {
        if (err) {
            callback(err)
        } else if(data.Messages === undefined){
            callback('empty msg')
        }
        else {
           // do something
        }
    });
}


function sendEmail(toAddr, ccAddr, body, subject) {

    let emailParams = {
        Destination: {
            CcAddresses: ccAddr,
            ToAddresses: toAddr
        },
        Message: {
            Body: {
                Text: {
                    Charset: "UTF-8",
                    Data: body
                }
            },
            Subject: {
                Charset: 'UTF-8',
                Data: subject
            }
        },
        Source: emailFrom,
        ReplyToAddresses: [
            emialFrom,
        ],
    };

    let sendPromise = ses.sendEmail(emailParams).promise();

    sendPromise.then(
        function (data) {
            console.log(data.MessageId);
        }).catch(
        function (err) {
            console.error(err, err.stack);
        });
}


exports.handler = (event, context, callback) => {
    try{
        handleSqsMsg(trackingList, (err, res) => {
            if (err) callback(err);
            else{
            callback(null, res)
            }
        }
    }catch(e){
        callback(null, e)
    }
};


// use for test in local
// this.handler({}, {}, (err, res)=>{
//     if (err) console.log(err);
//     else console.log(res);
// });/**
