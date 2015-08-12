/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

$( document ).ready( function () {

    var client,
        destination;

    $( '#connect_form' ).submit( function ( e ) {

        e.preventDefault();

        var url = $( '#connect_url' ).val(),
            loginName = $( '#connect_login' ).val(),
            passcode = $( '#connect_passcode' ).val();
        
        destination = $( '#destination' ).val();

        client = AeroGear.Notifier({
            name: 'stomp',
            type: 'stompws',
            settings: {
                connectURL: url
            }
        }).clients.stomp;

        var debug = function ( str ) {
                $( '#debug' ).append( str + "\n" );
            },
            onconnect = function () {
                debug( 'connected to Stomp');
                $( '#connect' ).fadeOut({
                    duration: 'fast'
                });
                $( '#disconnect' ).fadeIn();
                $( '#send_form_input' ).removeAttr( 'disabled' );
                $( '#unsubscribe' ).fadeIn();

                client.debug( debug );
            
                var onsubscribe = function ( message ) {
                    $( '#messages' ).append( "<p>" + message.body + "</p>\n" );
                };
            
                client.subscribe({
                    address: destination,
                    callback: onsubscribe
                });
            };

        client.connect({
            login: loginName,
            password: passcode,
            onConnect: onconnect
        });
    });

    $( '#disconnect_form' ).submit( function ( e ) {
        
        e.preventDefault();

        var ondisconnect = function () {
            $( '#disconnect' ).fadeOut({
                duration: 'fast'
            });
            $( '#unsubscribe' ).fadeOut({
                duration: 'fast'
            });
            $( '#connect' ).fadeIn();
            $( '#send_form_input' ).attr( 'disabled', 'disabled' );
            $( '#messages' ).empty();
            $( '#debug' ).empty();
        };
            
        client.disconnect( ondisconnect );
    });
    
    $( '#unsubscribe_form' ).submit( function ( e ) {

        e.preventDefault();

        client.unsubscribe( [{ address: destination }] );
        
        $( '#unsubscribe' ).fadeOut({
            duration: 'fast'
        });
    });

    $( '#send_form' ).submit( function ( e ) {
        
        e.preventDefault();

        var text = $( '#send_form_input' ).val();
        if (text) {
            client.send( destination, text );
            $('#send_form_input').val( '' );
        }
    });

});
