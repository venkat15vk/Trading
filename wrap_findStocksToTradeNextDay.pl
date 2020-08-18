#!/usr/bin/perl

use strict;

my $today = `date +"%d-%m-%Y"`; chomp $today;

my ($date, $month, $year) = $today =~ /^(\d\d)-(\d\d)-(\d\d\d\d)$/;

print "Analysing data till date: 2020-$month-$date; finding stocks to trade for next business day\n";

system("/Users/vk/Desktop/SchoolAndResearch/StockResearch/Trading/bin/findStocksToTradeNextDay.py $date $month $year");

