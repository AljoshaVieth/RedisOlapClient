#!lua name=mylib

local function insertAuthor(authorId, authorJson, articleId)
    local exists = redis.call("EXISTS", "authors:" .. authorId)
    local insertionSucceeded = ""
    if exists == 0 then
        insertionSucceeded = redis.call("JSON.SET", "authors:" .. authorId, "$", authorJson)
        redis.call("ZINCRBY", "author_article_count", 1, authorId)
        redis.call("PFADD", "author_hyperloglog_count", authorId)
    else
        insertionSucceeded = redis.call("JSON.ARRAPPEND", "authors:" .. authorId, "$.articles", articleId)
        redis.call("ZINCRBY", "author_article_count", 1, authorId)
        redis.call("PFADD", "author_hyperloglog_count", authorId)
    end
    return insertionSucceeded
end

local function insertArticle(id, json)
    return redis.call("JSON.SET", "articles:" .. id, "$", json)
end

local function insertData(keys, args)
    insertArticle(keys[1], args[1])
    -- insert authors
    for i = 2, #keys do
        insertAuthor(keys[i], args[i], keys[1])
    end
end

local function getArticlesOfAuthor(keys, args)
    local authorsResponse = redis.call("JSON.GET", "authors:" .. keys[1], "$.articles")
    local authorsJson = cjson.decode(authorsResponse)[1] -- access the json list of the response without the brackets
    local articles = {}
    for i = 1, #authorsJson do
        articles[i] = redis.call("JSON.GET", "articles:" .. authorsJson[i], "$")
    end
    return articles
end

local function getReferencesOfArticle(keys, args)
    local referencesResponse = redis.call("JSON.GET", "articles:" .. keys[1], "$.references")
    local referencesJson = cjson.decode(referencesResponse)[1] -- access the json list of the response without the brackets
    local references = {}
    for i = 1, #referencesJson do
        references[i] = redis.call("JSON.GET", "articles:" .. referencesJson[i], "$")
    end
    return references
end

local function getAuthorsWithMostArticles()
    local elementWithHighestScore = redis.call("ZRANGE", "author_article_count", 0, 0, "withscores", "rev")
    local highestScore = elementWithHighestScore[2]
    local authorsWithMostArticles = redis.call("ZRANGE", "author_article_count", highestScore, highestScore, "BYSCORE")
    -- creating one table with all arguments that will be passed to redis.call because redis.call only allows separate
    -- arguments, so the only way to dynamically insert arguments is storing them in a table and then unpacking this table.
    -- Since the function unpack() always unpacks the amount of elements that the statement (redis.call) expects at this position,
    -- it would only unpack the first element, if it is not the only statement in the redis.call() function.
    -- So the only way ist to store all arguments of the redis.call() function in a table and unpack everything.
    -- http://www.lua.org/pil/5.1.html
    local receiveAuthorsQuery = {"JSON.MGET"}
    for i = 1, #authorsWithMostArticles do
        receiveAuthorsQuery[i+1] = "authors:"..authorsWithMostArticles[i]
    end
    receiveAuthorsQuery[#receiveAuthorsQuery+1] = "$"
    return redis.call(unpack(receiveAuthorsQuery))
end

redis.register_function('insertAuthor', insertAuthor)
redis.register_function('insertArticle', insertArticle)
redis.register_function('insertData', insertData)
redis.register_function('getArticlesOfAuthor', getArticlesOfAuthor)
redis.register_function('getReferencesOfArticle', getReferencesOfArticle)
redis.register_function('getAuthorsWithMostArticles', getAuthorsWithMostArticles)



