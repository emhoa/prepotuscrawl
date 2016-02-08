#!/usr/bin/env python
import happybase

connection = happybase.Connection('52.72.203.8')
connection.open()

from flask import render_template
from flask import request
from flask import Markup


from app import app
@app.route('/')
def index():
        return render_template("namecrawler.html")

from app import app
@app.route("/", methods=['POST'])
def candidateblock():
        candidate = request.form["candidate"]
        fullcandidate = "cc:" + candidate
        numrecords = request.form["numrecords"]
        mytable = connection.table('candidate_crawl1')
        mystring = ""
        for key, data in mytable.scan(row_start=None, row_stop=None, row_prefix=None, columns={fullcandidate}, filter=None, timestamp=None, include_timestamp=False, batch_size=1000, scan_batching=None, limit=int(numrecords), sorted_columns=False):
                mystring += "<a href='" + key + "'>" + key + "</a><br>"
        print mystring
        return render_template('namecrawlerresults.html', output=Markup(mystring))



