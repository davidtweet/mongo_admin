#!/usr/bin/env ruby
# Copyright 2016 David Tweet.  All rights reserved.
# Use of this source code is governed by the Apache 2.0
# license which can be found in the LICENSE file.

require 'colorize'
require 'logger'
require 'mongo'
require 'optparse'
require 'socket'
require 'timeout'


class FormattedAttribute
    def initialize(header, url_to_attribute_map, format_functions)
        @header = header
        @url_to_attribute_map = url_to_attribute_map
        @format_functions = format_functions
        @max_length = self.get_max_length()
    end

    def get_max_length
        (@url_to_attribute_map.values.map{|v| v.length} + [@header.length]).max
    end

    def formatted_url_to_attribute_map
        return_map = {}
        @url_to_attribute_map.each do |k, v|
            return_map[k] = v.ljust(@max_length + 2)
            @format_functions.each do |f|
                return_map[k] = f.call(return_map[k])
            end
        end
        return_map
    end

    def formatted_header
        @header.ljust(@max_length + 2).upcase
    end
end
        

class MongoMonitor
    def initialize(instance_urls, target_db, refresh_seconds)
        @instance_urls = instance_urls
        @unreachable_urls = []
        @target_db = target_db
        @refresh_seconds = refresh_seconds
        @clients = {}
    end

    def run_top()
        while true
            attributes = [  
               FormattedAttribute.new(
                   'url',
                   Hash[@instance_urls.map{|u| [u, u]}],
                   []
               ),
               FormattedAttribute.new(
                   'is_reachable',
                   Hash[@instance_urls.map{|u| [u, self.get_reachability(u)]}],
                   [lambda {|x| /Yes/ =~ x ? x.green : x.red}]
               ),
               FormattedAttribute.new(
                   'cluster_status',
                   Hash[@instance_urls.map{|u| [u, self.get_cluster_status(u)]}],
                   [lambda {|x| /master/ =~ x ? x.black.on_cyan : x},
                   lambda {|x| /neither/ =~ x ? x.red : x}]
               ),
               FormattedAttribute.new(
                   'version',
                   Hash[@instance_urls.map{|u| [u, self.get_mongo_version(u)]}],
                   []
               ),
               FormattedAttribute.new(
                   "size_of_db[#{@target_db}]",
                   Hash[@instance_urls.map{|u| [u, self.get_database_size(u)]}],
                   []
               )
            ]
            system 'clear'
            attributes.each do |attr|
                print attr.formatted_header
            end
            puts
            @instance_urls.each do |url|
                attributes.each do |attr|
                    print attr.formatted_url_to_attribute_map[url]
                end
                puts
            end
            puts @unreachable_urls
            sleep(@refresh_seconds)
        end
    end

    def get_client(url)
        if !@clients.has_key?(url) then
            begin
                @clients[url] = Mongo::Client.new([url], :connect => :direct, :connect_timeout => 1.0, :server_selection_timeout => 1.0)
                @unreachable_urls.delete(url)
            rescue Mongo::Error::NoServerAvailable, Mongo::Error::SocketError
                @unreachable_urls << url
                @clients.delete(url)
            end
        end
        @clients[url] 
    end

    def run_database_command(url, query)
        begin
            client = self.get_client(url)
            database = client.database
            return database.command(query)
        rescue Mongo::Error::NoServerAvailable, Mongo::Error::SocketError
            @unreachable_urls << url
            @clients.delete(url)
            return nil
        end
    end

    def get_reachability(url)
        self.get_client(url)
        if @unreachable_urls.include?(url)
            return 'No'
        end
        return 'Yes'
    end
    
    def get_cluster_status(url)
        result = self.run_database_command(url, {:ismaster => 1})
        if @unreachable_urls.include?(url) then 
            return '-'
        end
        if result.first['ismaster'] then
            return 'master'
        elsif result.first['secondary'] then
            return 'secondary'
        else
            return 'neither'
        end
    end

    def get_mongo_version(url)
        result = self.run_database_command(url, {:buildInfo => 1})
        if @unreachable_urls.include?(url) then 
            return '-'
        end
        return result.first['version']
    end

    def get_database_size(url)
        result = self.run_database_command(url, {:listDatabases => 1})
        if @unreachable_urls.include?(url) then 
            return '-'
        end
        result.first['databases'].each do |db|
            if db['name'] == @target_db then
                return db['sizeOnDisk'].to_s
            end
        end
        return 'n/a'
    end
end


String.disable_colorization = false
Mongo::Logger.logger.level = Logger::WARN

options = {:refresh_secs => '5', :db => 'local'}
OptionParser.new do |opts|
    opts.banner = 'Usage: ./mongo_admin.rb [options] instance_1_url instance_2_url instance_3_url...'
    opts.on('-d', '--db DB', 'Database to inspect for size/existence', ) do |db|
        options[:db] = db
    end
    opts.on('-s', '--refresh_seconds SECONDS', 'Number of seconds to sleep before refresh') do |seconds|
        options[:refresh_secs] = seconds
    end
end.parse!

urls = ARGV

if urls.empty? then
    puts "URL list is required. Use -h for options"
    exit(1)
end

mm = MongoMonitor.new(urls, options[:db], options[:refresh_secs].to_i) 
mm.run_top()
