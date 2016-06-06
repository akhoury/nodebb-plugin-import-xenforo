var fs = require('fs-extra');

require('./index').testrun({
    dbhost: '127.0.0.1',
    dbport: 3306,
    dbname: 'xenforo_tiny',
    dbuser: 'user',
    dbpass: 'password',
    tablePrefix: 'xf_',
    custom: {
        "attachmentsSourceDirFullPath": "",  // required to get the attachemtns
        "attachmentsTargetDirFullPath": "", // optional, defaults to /your/nodebb/path/public/_imported_xf_attachments/
        "attachmentsTargetDirBaseUrl": "",  // optional, defalts to /_imported_xf_attachments/

		"avatarsCheckExistence": true
    }
}, function(err, results) {
    // fs.writeFileSync('./tmp.json', JSON.stringify(results, undefined, 2));
});