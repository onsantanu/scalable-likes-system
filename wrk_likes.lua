-- random user/post; 70% on 3 hot posts; 30% unlike
math.randomseed(os.time() + tonumber(tostring({}):sub(8), 16))
request = function()
  local post
  if math.random() < 0.7 then post = math.random(1, 3) else post = math.random(1, 100) end
  local user = math.random(1, 5000)
  local method = (math.random() < 0.3) and "DELETE" or "POST"
  return wrk.format(method, "/posts/" .. post .. "/like", {["X-User-Id"] = tostring(user)})
end
