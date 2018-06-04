LIMIT = 10000000
def check_token(client,token)
  client.hget('login:',token)
end

def update_token(client,token,user,item = nil)
  time_stamp = Time.now.to_i
  client.hset('login:',token,user)
  client.zadd('recent:',time_stamp,token)
  if item
    client.zadd("viewed:#{token}",time_stamp,item)
    client.zremrangebyrank("viewed:#{token}",0,-26)
    # with the most-viewed item having the lowest score,
    # and thus having an index of 0. 
    client.zincrby("viewed:",-1,item)
  end
end

def clean_full_sessions(client)
  size = 0
  while size <= LIMIT do
    size = client.zcard('recent:')
    sleep 2 if size <= LIMIT
    next
  end

  end_index = [size,LIMIT].min - 1
  tokens = client.zrange('recent:', 0, end_index)

  session_keys = tokens.inject([]){  |session_keys,token|
    ["viewed:","cart:"].each {|key|session_keys << "#{key}#{token}"}
  }

  client.del(session_keys)
  client.hdel("login:",tokens)
  client.zrem("recent:",tokens)
  
end

def add_to_cart(client,session,item,count)
  if count <= 0
    client.hdel("cart:#{session}",item)
  else
    client.hset("cart:#{session}",item,count)
  end
end

def cache_request(client,request,callback)
  # if we can't cache the request, immediately call the callback
  return callback(request) unless can_cache(client,request)
  # convert the request to a simple string key for later lookups
  page_token = hash_request(request)
  page_key = "cache:#{page_token}"
  # lookup content
  content = client.get(page_key)

  unless content
    content = callback(request)
    client.setex(page_key,200,content)
  end

  return content
end

def schedule_row_cache(client,row_id,delay)
  now = Time.now.to_i

  client.zadd("delay:",delay,row_id)
  client.zadd("schedule:",now,row_id)

end

def cache_rows(client)
  row = nil
  while row == nil do
    next_item = client.zrange("schedule:",0,0,with_scores: true)
    now = Time.now.to_i
    if next_item or next_item[0][1] < now
      sleep 0.05
      next
    end
    row_id = next_item[0][0]
    delay = client.zscore("delay:",row_id)
    if dealy <= 0
      client.zrem("schedule:",row_id)
      client.zrem("delay:",row_id)
      client.del("inv:#{row_id}")
      next
    end
    # get database row
    row = inventory.find(row_id)
  end
  
  client.zadd("schedule:",now + delay,row_id)
  # hypothesize that the row is a hash,so JSON.dump(row) to json
  client.set("inv:#{row_id}",JSON.dump(row))
end

def rescale_viewed(client,dead_loop = true)
  while dead_loop do
    client.zremrangebyrank("viewed:",20000,-1)
    client.zinterstore("viewed:",["viewed:"],weights: [0.5])
    sleep 300
  end
end

def can_cache(client,request)
  item_id = extract_item_id(request)
  return false if !item_id or is_dynamic(request)
  
  rank = client.zrank("viewed:",item_id)
  return "rank is not nil and rank < 10000"
end
