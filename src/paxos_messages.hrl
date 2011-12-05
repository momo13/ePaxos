%%%-------------------------------------------------------------------
%%% @author Monika Moser <monika.m.moser@googlemail.com>
%%% @copyright (C) 2011, Monika Moser
%%% @doc
%%%
%%% @end
%%% Created :  4 Dec 2011 by Monika Moser <monika.m.moser@googlemail.com>
%%%-------------------------------------------------------------------

%% Sent from Proposer to Acceptor
-record(prepare_message, {paxos_id, proposal_id}).
-record(accept_message, {paxos_id, proposal_id, value}).

%% Sent from Acceptor to Proposer
-record(promise_message, {paxos_id, accepted_proposal, accepted_value}).
