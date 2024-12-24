import './style.css';
import { pubSubClient } from 'streamlit-client';

document.querySelector<HTMLDivElement>('#app')!.innerHTML = `
  <div>
    <button type="button">Click</button>

    <button type="button">Click</button>
  </div>
`;

const client = pubSubClient<{
    messages: 'testmessage' | 'testmessage2';
}>({
    baseUrl: 'https://localhost:8000',
    path: '/events',
});

const test = client.namespace('messages').key('test');

const subscriber = test.subscribe();

subscriber.onError(() => {
    console.log('Error occurred');
    // subscriber.unsubscribe();
});

subscriber.onConnect(() => {
    console.log('Connected!');
});

subscriber.on('testmessage', (data) => console.log(data));

subscriber.on('testmessage2', (data) => console.log(data));

const [button, button2] = document.querySelectorAll('button');

button!.addEventListener('click', () => {
    test.publish(
        'testmessage',
        JSON.stringify({
            content: 'Hello',
        })
    );
});

button2!.addEventListener('click', () => {
    test.publish(
        'testmessage2',
        JSON.stringify({
            content: 'Hello',
        })
    );
});
