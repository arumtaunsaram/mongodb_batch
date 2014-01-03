"use strict";
var 
	_ = require('underscore'),
	RSVP = require('rsvp'),
	MongoClient = require('mongodb').MongoClient
	;

module.exports = {
	/**
		@param pmDB {string}
		@param pmCollection {string}
		@param 
			{Object.<{method: string, params: string, data: Array.<Object>, parser: function(doc)}>} pmObj
		@example
		parallel({method: 'find', params:'[{datetime:<datetime>, kind: <kind>}]', [{datetime:, kind:}...})
	 */
	parallel: function (pmDB, pmCollection, pmObj) {

		// TODO: Check parameters.

		connect({DB: pmDB}, pmCollection, function (pmErr, pmCol) {
		
			var 
				query = null,
				arrPromises = []
			;
			if (pmErr) {
				process.stderr.write(
					'Could not connect to the specified collection, ' + pmDB + '/' + pmCollection + '.'
				);
				return;
			}

			for (var i = 0, max = pmObj.data.length; i < max; i += 1) {

				query = [];

				for (var nth = 0, nmax = pmObj.params.length; nth < nmax; nth += 1) {
					query.push(
						_.defaults(
							(typeof pmObj.data[ i ][ nth ] === 'object')? pmObj.data[ i ][ nth ] : {},
							pmObj.params[ nth ]));
				}

				arrPromises.push(new RSVP.Promise(function (pmFill, pmReject) {
					execute(pmCol, pmObj.method, query, pmFill, pmReject);
				}));
			}

			RSVP.all(arrPromises).then(function (pmDocs) {

				for (var i = 0, max = pmDocs.length; i < max; i += 1) {
					pmObj.parser(pmDocs[ i ]);
				}

				process.exit(0);

			}).then(function (pmErrors) {
				console.error(pmErrors);
			})
		})

	}
};

// Internal functions.

/**

	@param {Object.<{URI: string?, DB: string?, collection: string}>} pmDescriptor
	@param {string} pmCollection
	
 */
function connect (pmDescriptor, pmCollection, pmCb) {

	var host = (typeof pmDescriptor['URI'] === 'string')?	
					pmDescriptor['URI'] : 'mongodb://localhost/' + pmDescriptor['DB'];

	MongoClient.connect(host, function (err, db) {
		if (err) {
			process.stderr.write(
				'Could not connect to the DB, ' + host + '.'
			);
			return;
		}
		db.collection(pmCollection, function (err, col) {pmCb(err, col, db)});
	});
}

function execute (pmCol, pmMethod, pmQueryParams, pmFill, pmReject) {

	pmCol[ pmMethod ]
		.apply(pmCol, pmQueryParams)
		.toArray(function (err, doc) {
			if (err) {
				pmReject(err);
			} else {
				pmFill(doc);
			}
	});
}
