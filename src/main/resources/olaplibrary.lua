#!lua name=olaplib

local function querySpecificDocuments(keys, args)
    return redis.call("FT.SEARCH", "date-index", "@d_year:[1993 1993]", "LIMIT", 0, 0)
end

redis.register_function('querySpecificDocuments', querySpecificDocuments)



