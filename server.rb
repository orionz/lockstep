require 'sinatra'
require 'json'

get "/servers/:id" do
  {id: 123, updated_at: 999, deleted_at: nil, ip: "0.0.0.0", port: 1234}.to_json + "\n" +
  {id: 124, updated_at: 1000, deleted_at: 1000, ip: "0.0.0.0", port: 1234}.to_json + "\n"
end
