#!/usr/bin/perl

use strict;

my @dates = (13);
my $month = 8;

for my $date (@dates){
	system("/Users/vk/Desktop/SchoolAndResearch/StockResearch/Trading/bin/findStocksToTradeNextDay.py $date $month");

}
