const express = require("express")
const { Client } = require("pg")
var cors = require("cors")
const bodyParser = require("body-parser")
const router = express.Router()

const client = new Client({
	user: "postgres",
	host: "localhost",
	database: "wiki",
	password: "1",
	port: 5432,
})
client.connect()

const PORT = process.env.PORT || 3001

const app = express()

app.use(cors())

app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())

router.get("/api", (req, res) => {
	const query = req.query
	var categoryList
	var includeCategories
	if ("categories" in query) {
		categoryList = query.categories.toString()
		includeCategories = true
	} else {
		categoryList = ""
		includeCategories = false
	}

	var sqlQuery = `select *, count(*) OVER()  from ${query.tfidf}('${query.query}', '{${categoryList}}'::varchar[], ${includeCategories}) limit ${query.numArticles}`
	// var sqlQuery = `select *   from ${query.tfidf}('${query.query}', '{${categoryList}}'::varchar[], ${includeCategories}) limit ${query.numArticles}`
	console.log(sqlQuery)

	client.query(sqlQuery).then((result) => {
		res.json({ message: result.rows })
	})
})

router.get("/results", (req, res) => {
	var sqlQuery = `select * from search_result`

	console.log(sqlQuery)

	client.query(sqlQuery).then((result) => {
		res.json({ message: result.rows })
	})
})

router.post("/searchstats", (req, res) => {
	var data = req.body

	var sqlQuery = `INSERT INTO search_result(query, function, time, articleCount, categories)		VALUES ('${
		data.query
	}', '${data.tfidf}', '${data.time}', '${
		data.articleCount
	}', '{${data.categories.join(", ")}}')`

	console.log(sqlQuery)

	client.query(sqlQuery).then((result) => {
		res.status(201).send(`item added`)
	})
})

app.use("/", router)

app.listen(PORT, () => {
	console.log(`Server listening on ${PORT}`)
})
