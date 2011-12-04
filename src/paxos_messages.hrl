%%%-------------------------------------------------------------------
%%% @author Monika Moser <monika.m.moser@googlemail.com>
%%% @copyright 2011, Monika Moser
%%% @doc Messages sent between paxos participants
%%% @end
%%%-------------------------------------------------------------------

-record(prepare_message, {paxos_id, proposal_id}).