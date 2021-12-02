#Nome: Kaio Nakazono
#Teste Zimobi: https://gist.github.com/douglara/04ddc5341e02e79e859d5b17b77af40e

require 'uri'
require 'net/http'
require 'openssl'
require 'json'

@users_size = 0

def millisecondsToMinutes(value)
   temp = value / 1000
   temp /= 60
   return temp
end

def getRequest(url_request)
   url = URI(url_request)

   http = Net::HTTP.new(url.host, url.port)
   http.use_ssl = true
   http.verify_mode = OpenSSL::SSL::VERIFY_NONE

   request = Net::HTTP::Get.new(url)


   response = http.request(request)
   temp = JSON.parse(response.read_body)
   return temp
end

#Obtem os usuário, elimando usuários duplicados
def getUsers(data)
   users = []
   data['events'].size.times do |i|
      users << data['events'][i]['visitorId']
   end

   users.uniq! #tira users duplicados
   return users
end

def organizaDados(url_data)
   sessionsByUser = []
   data = getRequest(url_data)
   users = getUsers(data)
   @users_size = users.size
   users.size.times do |i|
      temp_sessions = []
      temp_sessions << users[i]
      data['events'].size.times do |j|
         if data['events'][j]['visitorId'] == users[i]
            #Organiza os dados e insere na var temp_sessions
            temp = {url: data['events'][j]['url'], timestamp: data['events'][j]['timestamp']}
            temp_sessions << temp
         end
      end
      #Insere os dados em sessionsByUser
      sessionsByUser << temp_sessions
   end

   #ordena pelo timestamp
   data_organizado = []
   i = 0
   while i < users.size
      temp_user = sessionsByUser[i][0] #Guarda o usuário na variável 'temp_user'
      temp_hash = sessionsByUser[i][1..(sessionsByUser[i].size - 1)] #Extrai apenas os eventos e guarda na variável 'temp_hash' (que é um array de hashes)
      temp_hash.sort_by!{|k| k[:timestamp]} #Ordena o array de hashes em ordem cronológica pelo timestamp

      temp_data = []
      temp_data << temp_user
      temp_hash.each do |j|
         temp_data << j
      end
      data_organizado << temp_data
      i += 1
   end
   return data_organizado
end

def resolve(array, i)
   startTime = 0
   duration = 0
   pages = []
   events = {}
   
   #A variável 'data' estará no formato: ['user1', {event1}, {event2}, ...]. Assim: data[0] == 'user1', data[1] == event1, ...
   data = []
   data << array[i][0] #insere o usuário na primeira posição do data

   limit = 10 #10 minutes
   size = array[i].size - 1 #tira o user
   j = 1
   while j <= size
      if j == 1
         startTime = array[i][j][:timestamp]
      end
      if millisecondsToMinutes(array[i][j][:timestamp] - startTime) < limit
         pages << array[i][j][:url]

         #Se ultimo evento
         if j == size
            duration = array[i][j][:timestamp] - startTime 
            pages.size > 1 ? events[:duration] = duration : events[:duration] = 0
            events[:pages] = pages
            events[:startTime] = startTime
            data << events
         end
      
      #Irá iniciar outra Session
      else 
         duration = array[i][j-1][:timestamp] - startTime #calcula a duração da session anterior
         pages.size > 1 ? events[:duration] = duration : events[:duration] = 0 #se page < 1, então só teve apenas um evento (só ficou em uma página durante mais de 10 minutos (como eu sei que ficou mais de 10 minutos? pq entrou aqui no else, que será outra sessão))
         events[:pages] = pages
         events[:startTime] = startTime
         data << events

         ################## Nova Session ######################
         #Atualiza startTime e limpa os arrays pages e events
         startTime = array[i][j][:timestamp]
         pages = [] #não fazer 'pages.clear', senão apaga o array pages do hash events[:pages]
         events = {}
         
         pages << array[i][j][:url]

         #Se ultimo evento
         if j == size
            duration = array[i][j][:timestamp] - startTime 
            pages.size > 1 ? events[:duration] = duration : events[:duration] = 0
            events[:pages] = pages
            events[:startTime] = startTime
            data << events
         end

      end
      j += 1
   end
   return data
end

def sessionsUsers(url_data)
   temp_hash = {}
   temp_array = []
   resp = {}
   data = organizaDados(url_data)
   i = 0
   j = 0
   while i < @users_size
      #temp_array estará no formato: [][]
      temp_array << resolve(data, i)
      i += 1
   end

   while j < temp_array.size
      #'hash'.store('key', 'value'), funciona como um 'append'
      #temp_hash ficará no formato {{key, value}, {key, value}, ...}
      temp_hash.store(temp_array[j][0], temp_array[j][1..(temp_array[j].size-1)])
      j += 1
   end

   resp['sessionsByUser'] = temp_hash
   
   return resp
end

###### Teste ######

url_data = "https://my-json-server.typicode.com/masaru1405/dataset_web_analytic/db"


resposta = sessionsUsers(url_data)
puts resposta



