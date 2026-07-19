import http from 'k6/http';
import { check, sleep } from 'k6';

export const options = {
    scenarios: {
        donation_test: {
            executor: 'shared-iterations',
            vus: 50,            // number of concurrent VUs to use
            iterations: 10000,  // total requests across all VUs
            maxDuration: '30m', // safety timeout
        },
    },

    thresholds: {
        http_req_failed: ['rate<0.01'],       // Less than 1% failures
        http_req_duration: ['p(95)<500'],     // 95% of requests under 500ms
    },
};

const bloodGroups = [
    "A+",
    "A-",
    "B+",
    "B-",
    "AB+",
    "AB-",
    "O+",
    "O-"
];

export default function () {

    const donorId = Math.floor(Math.random() * 1000000);

    const body = JSON.stringify({
        donor_id: donorId,
        blood_bank_id: Math.floor(Math.random() * 5) + 1,
        blood_group: bloodGroups[Math.floor(Math.random() * bloodGroups.length)],
        units_donated: Math.floor(Math.random() * 3) + 1
    });

    const params = {
        headers: {
            "Content-Type": "application/json",
        },
    };

    const response = http.post(
        "http://localhost:8000/donations",
        body,
        params
    );

    check(response, {
        "Status is 200": (r) => r.status === 200,
    });

    sleep(0.1);
}